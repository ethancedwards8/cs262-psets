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


Notebook:

Design:

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

Obviously the pt-paxos code assumes that these will be a single type. I wanted to avoid special c++
features and used something like a single type with an enum to switch on logic, but it seems somewhat
impractical? I'm not sure. Anyways, LLMs seem to think that using std::variant is the way to go.

```c++
using paxos_message = std::variant<prepare_paxos_msg, ack_paxos_msg>;
```