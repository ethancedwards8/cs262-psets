Problem set 3 turnin
====================

Describe your turnin. What features did you add? And make sure you fill out the
“Bug” section!

(You can also keep a lab notebook in NOTEBOOK.md.)

Bug
---

At some point during your work, you will have found a bug in your
implementation. D this bug, give a commit hash that demonstrates the bug, and
describe how you found it and how you fixed it. Give specific pt-paxos arguments
that demonstrate the error.

Bug! See below in the Bug section in phase 2 :)


# Notebook:

## Phase 1 / Design:

I want to think about good optimizations from the beginning in order to reduce the
amount of churn, even if it lead to some pain during the development process.

I also want to draw a lot of inspiration from Raft. From my understanding, Docker Swarm
and Hashicorp Nomad (maybe others too) use Raft-based protocols to implement log replication.

To get through the first two phases, this was my thought for the design.

This is my design I think:

Imagine the log as a continuous stream:

slots left to right advancing in time
<========================================>
leftmost slots - decided and committed slots on quorom
in the middle, accepted slots for the round that have need to be quoromed
on the right, in flight candidate slots

<========================================>
[committed][accepted][tbd in flight]

and then as time goes on the log can be shorted because all replicas agree on it.

however, if one replica is down it cannot be shortened. This is also related to the view change problem.

One solution is to use snapshots and send the full snapshot. I think the smartest thing to do is cut the
log if one replica is down for a sufficient amount of time and instead send it over if it comes back or
so that a new replica can join (view change).

But the essential message types so far:

```cpp
struct base_message {
    unsigned long long round;
}

// EDIT: halfway through i realized im
// a dumbass and this is propose i just wasn't thinking straight
// i realize this at the beginning of phase 3.
// but anyways leaving here for posterity
// and for you, the grader, to laugh
// and my dumbass.
// anyways this is also proof
// im not over-relying on AI
// because I'm pretty sure it
// would've caught this!
struct prepare_msg : base_message {
    unsigned long long committed_slot = 0; // decide shortcut
    unsigned leader_id = 0;

    unsigned long long prev_slot;
    unsigned long long prev_round;

    unsigned long long batch_start = 0;
    std::deque<pancy_message> entries; // empty = raft heartbeat keepalive
}

struct ack_msg : base_message {
    bool success = false;
    unsigned long long highest_accepted = 0;
}
```

Jonathan mentioned using a Deque over a Vector for easier truncation and appending purposes

Obviously the pt-paxos code assumes that these will be a single type. I wanted to avoid special c++
features and used something like a single type with an enum to switch on logic, but it seems somewhat
impractical? I'm not sure. Anyways, LLMs seem to think that using std::variant is the way to go.

```c++
using paxos_message = std::variant<prepare_paxos_msg, ack_paxos_msg>;
```


From there, separating out the logic for leaders/replicas makes things easier to read and reason about:

Leader:

Leader recv. client request, fans out req to followers.

Waits for a quorum of followers to send acks back, applies req to db if quorom
reached, and then replies to client.


Follower:

Takes req from leader, processes it and sends ack.


## Phase 2

In order to do replica message retry, I looked at the lockseq_model.cc file and observed how it did it.

```c++
            co_await send_request<pancy::cas_request>(
                cs.leader, serial, lock_key, "", value
            );
            auto resp = co_await cot::attempt(
                receive_response<pancy::cas_response>(cs.leader, serial),
                cot::after(randomness().normal(3s, 1s))
            );
            if (!resp) {
                continue;
            }
```

From there, I modeled my implementation similarly.

Of course. I quickly hit a bug (or at least something that confuses me):

My first guess is something to do with receive_with_id(). I saw the ed post, but
I'm not sure if this is it? I think the issue is fixed based off of commit
messages.

This is visible at commit: 61a88c2bbbb1da8f447e239873c373012b707f62 

```
cs61-user@f27e0641ebd3:~/cs2620/pset3$ build/pt-paxos -n 3 -l .1
42 lock, 19 write, 17 clear, 17 unlock
cs61-user@f27e0641ebd3:~/cs2620/pset3$ build/pt-paxos -n 3 -l .1
43 lock, 16 write, 16 clear, 16 unlock
cs61-user@f27e0641ebd3:~/cs2620/pset3$ build/pt-paxos -n 3 -l .1
36 lock, 11 write, 11 clear, 11 unlock
cs61-user@f27e0641ebd3:~/cs2620/pset3$ build/pt-paxos -n 2 -l .1
25 lock, 0 write, 0 clear, 0 unlock
cs61-user@f27e0641ebd3:~/cs2620/pset3$ build/pt-paxos -n 2 -l .1
28 lock, 2 write, 0 clear, 0 unlock
cs61-user@f27e0641ebd3:~/cs2620/pset3$ build/pt-paxos -n 2 -l .1
27 lock, 0 write, 0 clear, 0 unlock
cs61-user@f27e0641ebd3:~/cs2620/pset3$ build/pt-paxos -n 4 -l .1
28 lock, 7 write, 4 clear, 4 unlock
cs61-user@f27e0641ebd3:~/cs2620/pset3$ build/pt-paxos -n 4 -l .1
27 lock, 2 write, 0 clear, 0 unlock
cs61-user@f27e0641ebd3:~/cs2620/pset3$ build/pt-paxos -n 4 -l .1
25 lock, 2 write, 0 clear, 0 unlock
cs61-user@f27e0641ebd3:~/cs2620/pset3$ build/pt-paxos -n 4 -l .1
27 lock, 2 write, 0 clear, 0 unlock
cs61-user@f27e0641ebd3:~/cs2620/pset3$ build/pt-paxos -n 4 -l .1
26 lock, 3 write, 1 clear, 1 unlock
```

I am not really sure why things were failing to work at a n=2 with 10% loss but
I tested up to n = 8 and [1, 8] worked except for n=2.

I added print statements through the code to create bad-performance-trace.txt.

I also used a visualizer tool to think through whats happening:
build/pt-paxos -n 2 -l .1 -S 289372592735 -V > output.txt
python3 ./paxosvis.py ./output.txt > bug.html // thanks claude for the viz tool!

After being confused about client redirections for a second, I ran it with -R 200
and noticed one time (!!!!) where unlock reached 1. Hmmm. Red flag...

Then I added a delay to the redirect because I was sus of it. Then confused myself
because they overlapped with the actual requests. Eventually figured it out.

Anyways, it seems that the 10% loss is just really effective. Who would've thought that
a loss of 10% would run about 10% of the time? wow. genius me.

Anyways, two cases were happening:
the prepare was making it to the other replica but the ack never made it.
or the prepare is sent but never makes it.

In both cases, I had a 1 second delay wait before sending another message. That is probably
too high I think? I'm not exactly sure. But because the whole simulation is limited to 100s,
it was quickly hitting 100s without having made a transaction.

This is a quirk of quorom when only 2 nodes are present. The whole protocol just
stalls at n=2 if it cant contact another node. At n=3, though, the leader can
reach quorom with only one response. So in order for something like this to
happen (and it can, I tested it with -R 2000 and got a few). Its basically (I think)
a 1% chance (because 10% * 10%). yay. fixed. pwned.

200ms seemed to be okay, but that may be too low? I'm not sure. maybe this bites me in the ass.

With that, I think (hope, pray) that phase 2 is done.

## Phase 3

so this requires more message types.

I have propose (combined decide), ack. and then oh shit. i realized i misnamed my propose as prepare.
so I will rename that real quick. oops. lmao if nothing else this is proof I'm not over-relying on AI.
I'm guessing it would have caught that.

anyways the logs i committed earlier also have this mistake so if you look at them keep that in mind.

FIXED! anyways, here we go:
i have propose and ack. yay.

I also need probe and and prepare message.

probe needs to only run when a leader assumes leadership (start of program or
when leader fails or ursuption i think)

prepare needs to reply and leader needs to agree that all values are equal across replicas.


but first i'm going to create a bunch of failure models (all usable via -f):
- split_brain (WIP)
- failed_leader
- failed_replica
- delayed_leader_failure
- multiple_random_up_down

anyways,

for my failure detector, i will add a heartbeat message that's sent at a consistent interval
(probably some even split of the failure detector timeout? not sure) that keeps the failure
detector from running during periods of low client traffic. if for some reason the failure
detector detects that it hasn't received a heartbeat or real client traffic at a given
interval then it will assume that the leader has died and assume leadership via probes with a higher round number.

after probes are recv and prepares are sent back to the replica that sent the probes, then
the new leader takes the highest round

we talked about this in class, though. each replica needs to have a slightly different
failure detector length so that not all failure detectors run at one time and every single
replica sends probes out.

for impl i started by just chekcing to see if the replica receives a message from the
heartbeat_interval_. (although i wonder if i should build in a buffer. eddie mentioned
that time is inconsistent in the real world but i think in this virtual time world its fine.
might be important in the next pset though? if we do real network traffic.)

also just sent heartbeats every heartbeat_interval_.

for explicit probe and prepare types:

```c++
// l -> f
struct probe_msg : base_message {
    unsigned leader_id = 0;
};

// f -> l
struct prepare_msg : base_message {
    unsigned long long accepted_round;
    std::deque<pancy::request> accepted_values;
}
```

At this point I feel pretty confident in my implementation and now I will try and break it.

Also there are a number of optimizations that my rpc supports that I haven't actually implemented yet like truncating the log.

And here I realized another bug fairly quickly.

Commit: d4532b9992d3ad7901bf5f9401c94bb7c3a2d298

```
build/pt-paxos -n 3 -r 2 -f multiple_random_up_down -S 2636664949796318461
131 lock, 120 write, 119 clear, 119 unlock
*** REPLICA DIVERGENCE on seed 2636664949796318461 between replica 0 and 1 at key g73/v003
   ...
   g72/v058 [V6088] c31 25ad5f7
   g73/lock [V7409] c25 c7e3c2b
   g73/v000 [V7419] c25 c7e3c2b
   g73/v001 [V7429] c25 c7e3c2b
   g73/v002 [V7441] c25 c7e3c2b
   g74/v000 [V5108] c18 c92ca56
   g74/v001 [V5125] c18 c92ca56
   g74/v002 [V5141] c18 c92ca56
   g74/v003 [V5159] c18 c92ca56
   g74/v004 [V5174] c18 c92ca56
   g74/v005 [V5191] c18 c92ca56
   ...
   ...
   g72/v058 [V6088] c31 25ad5f7
   g73/lock [V7409] c25 c7e3c2b
   g73/v000 [V7419] c25 c7e3c2b
   g73/v001 [V7429] c25 c7e3c2b
   g73/v002 [V7441] c25 c7e3c2b
 * g73/v003 [V7453] c25 c7e3c2b
   g74/v000 [V5108] c18 c92ca56
   g74/v001 [V5125] c18 c92ca56
   g74/v002 [V5141] c18 c92ca56
   g74/v003 [V5159] c18 c92ca56
   g74/v004 [V5174] c18 c92ca56
   ...
```

Basically what was happening is that I was not correctly truncating the log
after processing and the prepare messages being sent after a leader failover
were all being applied (from 0 to n entries). basically just a bunch of redoing.
