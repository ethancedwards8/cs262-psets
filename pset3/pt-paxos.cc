#include "lockseq_model.hh"
#include <algorithm>
#include <cstring>
#include "pancydb.hh"
#include "netsim.hh"
#include "paxos.hh"

namespace cot = cotamer;
using namespace std::chrono_literals;

// testinfo
//    Holds configuration information about this test.

enum failure_mode {
    failed_leader,
    failed_replica,
    multiple_random_up_down,
    split_brain,
    delayed_leader_failure,
    none,
};

struct testinfo {
    random_source randomness;
    double loss = 0.0;
    bool verbose = false;
    bool print_db = false;
    size_t nreplicas = 3;
    size_t initial_leader = 0;

    int failed_replica = -1;
    failure_mode mode = failure_mode::none;

    template <typename T>
    void configure_port(netsim::port<T>& port) {
        port.set_verbose(verbose);
    }
    template <typename T>
    void configure_channel(netsim::channel<T>& chan) {
        chan.set_loss(loss);
        chan.set_verbose(verbose);
    }
    template <typename T>
    void configure_quiet_channel(netsim::channel<T>& chan) {
        chan.set_loss(loss);
    }
};


// pt_paxos_replica, pt_paxos_instance
//    Manage a test of a Paxos-based Pancy service.
//    Initialization is more complicated than in the simpler settings;
//    we have a type, `pt_paxos_replica`, that represents a single replica,
//    and another, `pt_paxos_instance`, that constructs the replica set.

struct pt_paxos_instance;

// The type for inter-replica messages. You will change this!
// paxos_message is defined in paxos.hh

struct pt_paxos_replica {
    size_t index_;           // index of this replica in the replica set
    size_t nreplicas_;       // number of replicas
    size_t leader_index_;    // this replica’s idea of the current leader
    netsim::port<pancy::request> from_clients_;   // port for client messages
    netsim::port<paxos_message> from_replicas_;   // port for inter-replica messages
    netsim::channel<pancy::response> to_clients_; // channel for client responses
    // channels for inter-replica messages:
    std::vector<std::unique_ptr<netsim::channel<paxos_message>>> to_replicas_;
    pancy::pancydb db_;      // our copy of the database


    // ...plus anything you want to add
    unsigned long long next_round_ = 1;
    unsigned long long accepted_round_ = 0;
    std::deque<pancy::request> accepted_values_;
    unsigned long long commit_index_ = 0;
    unsigned long long applied_index_ = 0;
    std::vector<unsigned long long> match_index_;
    std::vector<unsigned long long> applied_up_to_;
    cot::duration heartbeat_interval_ = 200ms;
    cot::duration failure_timeout_;

    pt_paxos_replica(size_t index, size_t nreplicas, random_source&);
    void initialize(pt_paxos_instance&);

    cot::task<> run();
    cot::task<> run_as_leader();
    cot::task<> run_as_follower();
    cot::task<> send_to_other_replicas(const paxos_message& msg);

private: 
    unsigned long quorum_ = nreplicas_ / 2 + 1;
};

struct pt_paxos_instance {
    testinfo& tester;
    client_model& clients;
    std::vector<std::unique_ptr<pt_paxos_replica>> replicas;
    // ...plus anything you want to add

    pt_paxos_instance(testinfo&, client_model&);
};

static size_t replica_index_from_source_id(const std::string& source_id) {
    return from_str_chars<size_t>(source_id.substr(1));
}

static unsigned long long message_round(const paxos_message& msg) {
    return std::visit([](const auto& m) {
        return m.round;
    }, msg);
}


// Configuration and initialization

pt_paxos_replica::pt_paxos_replica(size_t index, size_t nreplicas, random_source& randomness)
    : index_(index),
      nreplicas_(nreplicas),
      from_clients_(randomness, std::format("R{}", index_)),
      from_replicas_(randomness, std::format("R{}/r", index_)),
      to_clients_(randomness, from_clients_.id()),
      to_replicas_(nreplicas),
      match_index_(nreplicas, 0),
      applied_up_to_(nreplicas, 0),
      failure_timeout_(randomness.uniform(800ms, 1200ms)) {
    for (size_t s = 0UL; s != nreplicas_; ++s) {
        to_replicas_[s].reset(new netsim::channel<paxos_message>(
            randomness, from_clients_.id()
        ));
    }
}

void pt_paxos_replica::initialize(pt_paxos_instance& inst) {
    leader_index_ = inst.tester.initial_leader;
    inst.clients.connect_replica(index_, from_clients_, to_clients_);
    inst.tester.configure_port(from_clients_);
    inst.tester.configure_port(from_replicas_);
    inst.tester.configure_channel(to_clients_);
    inst.tester.configure_quiet_channel(inst.clients.request_channel(index_));
    for (size_t s = 0UL; s != nreplicas_; ++s) {
        to_replicas_[s]->connect(inst.replicas[s]->from_replicas_);
        inst.tester.configure_channel(*to_replicas_[s]);
    }
}

pt_paxos_instance::pt_paxos_instance(testinfo& tester, client_model& clients)
    : tester(tester), clients(clients), replicas(tester.nreplicas) {
    for (size_t s = 0UL; s != tester.nreplicas; ++s) {
        replicas[s].reset(new pt_paxos_replica(s, tester.nreplicas, tester.randomness));
    }
    for (size_t s = 0UL; s != tester.nreplicas; ++s) {
        replicas[s]->initialize(*this);
    }
}



// ********** PANCY SERVICE CODE **********

cot::task<> pt_paxos_replica::run() {
    while (true) {
        if (index_ == leader_index_) {
            co_await run_as_leader();
        } else {
            co_await run_as_follower();
        }
    }
}

cot::task<> pt_paxos_replica::send_to_other_replicas(const paxos_message& msg) {
    for (size_t s = 0; s != nreplicas_; ++s) {
        if (s == index_)
            continue;

        co_await to_replicas_[s]->send(msg);
    }
}

cot::task<> pt_paxos_replica::run_as_leader() {
    probe_msg probe;
    probe.round = next_round_++;

    co_await send_to_other_replicas(probe);

    std::fill(match_index_.begin(), match_index_.end(), 0);
    std::fill(applied_up_to_.begin(), applied_up_to_.end(), 0);

    std::vector<bool> prepared(nreplicas_, false);
    size_t prepare_count = 0;
    unsigned long long highest_accepted_round = accepted_round_;
    std::deque<pancy::request> highest_accepted_values = accepted_values_;
    while (prepare_count < quorum_ - 1) {
        auto received = co_await cot::attempt(
            from_replicas_.receive_with_id(),
            cot::after(200ms)
        );

        if (!received) {
            co_await send_to_other_replicas(probe);
            continue;
        }

        auto& [paxos_msg, source_id] = *received;
        if (message_round(paxos_msg) > probe.round) {
            accepted_round_ = message_round(paxos_msg);
            next_round_ = std::max(next_round_, accepted_round_ + 1);
            leader_index_ = replica_index_from_source_id(source_id);
            co_return;
        }

        auto* prepare = std::get_if<prepare_msg>(&paxos_msg);
        if (!prepare)
            continue;

        if (prepare->round != probe.round)
            continue;

        size_t sender_index = replica_index_from_source_id(source_id);
        if (prepared[sender_index])
            continue;

        prepared[sender_index] = true;
        applied_up_to_[sender_index] = prepare->applied_up_to;

        if (prepare->accepted_round > highest_accepted_round) {
            highest_accepted_round = prepare->accepted_round;
            highest_accepted_values = prepare->accepted_values;
        }
        ++prepare_count;
    }

    // Adopt highest accepted values but do NOT apply to db yet
    if (highest_accepted_round > accepted_round_) {
        accepted_round_ = highest_accepted_round;
        accepted_values_ = highest_accepted_values;
    }
    match_index_[index_] = accepted_values_.size();
    applied_up_to_[index_] = applied_index_;

    std::deque<unsigned long long> pending_client_slots;
    std::deque<pancy::response> ready_client_responses;
    const size_t follower_quorum = quorum_ > 0 ? quorum_ - 1 : 0;

    while (true) {
        auto received = co_await cot::attempt(
            from_clients_.receive(),
            cot::after(heartbeat_interval_)
        );

        propose_msg propose;
        propose.round = next_round_++;
        propose.batch_start = accepted_values_.size();
        propose.committed_slot = commit_index_;

        if (received) {
            auto req = std::move(*received);
            pending_client_slots.push_back(propose.batch_start);
            accepted_values_.push_back(req);
            propose.entries.push_back(req);
            accepted_round_ = propose.round;
            match_index_[index_] = accepted_values_.size();
        }

        std::vector<bool> acked(nreplicas_, false);
        std::vector<propose_msg> in_flight(nreplicas_, propose);
        for (size_t s = 0; s != nreplicas_; ++s) {
            if (s == index_) {
                continue;
            }
            co_await to_replicas_[s]->send(in_flight[s]);
        }

        size_t ack_count = 0;
        while (ack_count < follower_quorum) {
            auto ack_received = co_await cot::attempt(
                from_replicas_.receive_with_id(),
                cot::after(200ms)
            );

            if (!ack_received) {
                for (size_t s = 0; s != nreplicas_; ++s) {
                    if (s == index_ || acked[s]) {
                        continue;
                    }
                    co_await to_replicas_[s]->send(in_flight[s]);
                }
                continue;
            }

            auto& [paxos_msg, source_id] = *ack_received;
            if (message_round(paxos_msg) > propose.round) {
                accepted_round_ = message_round(paxos_msg);
                next_round_ = std::max(next_round_, accepted_round_ + 1);
                leader_index_ = replica_index_from_source_id(source_id);
                co_return;
            }

            auto* ack = std::get_if<ack_msg>(&paxos_msg);

            if (!ack)
                continue;

            if (ack->round != propose.round)
                continue;

            size_t sender_index = replica_index_from_source_id(source_id);
            if (acked[sender_index])
                continue;

            if (!ack->success) {
                match_index_[sender_index] = ack->highest_accepted;
                applied_up_to_[sender_index] = ack->applied_up_to;

                auto& repair = in_flight[sender_index];
                repair.round = propose.round;
                repair.batch_start = ack->highest_accepted;
                repair.committed_slot = commit_index_;
                repair.entries.clear();
                for (size_t i = repair.batch_start; i != accepted_values_.size(); ++i) {
                    repair.entries.push_back(accepted_values_[i]);
                }
                co_await to_replicas_[sender_index]->send(repair);
                continue;
            }

            acked[sender_index] = true;
            match_index_[sender_index] = ack->highest_accepted;
            applied_up_to_[sender_index] = ack->applied_up_to;

            ++ack_count;
        }

        // Compute new commit index: highest index a quorum has reached
        auto sorted = match_index_;
        std::sort(sorted.begin(), sorted.end());
        commit_index_ = sorted[nreplicas_ - quorum_];

        unsigned long long new_applied = commit_index_;
        if (nreplicas_ > 1) {
            std::vector<unsigned long long> follower_applied;
            follower_applied.reserve(nreplicas_ - 1);
            for (size_t i = 0; i != nreplicas_; ++i) {
                if (i != index_) {
                    follower_applied.push_back(applied_up_to_[i]);
                }
            }
            std::sort(follower_applied.begin(), follower_applied.end());
            new_applied = std::min(
                commit_index_,
                follower_applied[follower_applied.size() - follower_quorum]
            );
        }

        for (auto i = applied_index_; i < new_applied; ++i) {
            auto resp = db_.process_req(accepted_values_[i]);
            if (!pending_client_slots.empty() && pending_client_slots.front() == i) {
                ready_client_responses.push_back(std::move(resp));
                pending_client_slots.pop_front();
            }
        }
        applied_index_ = new_applied;
        applied_up_to_[index_] = applied_index_;

        while (!ready_client_responses.empty()) {
            co_await to_clients_.send(std::move(ready_client_responses.front()));
            ready_client_responses.pop_front();
        }
    }
}

cot::task<> pt_paxos_replica::run_as_follower() {
    while (true) {
        auto msg = co_await cot::first(
            from_clients_.receive(),
            from_replicas_.receive_with_id(),
            cot::after(failure_timeout_)
        );

        auto* req = std::get_if<pancy::request>(&msg);
        if (req) {
            co_await cot::after(.02s); // to make it more obvious in the viz
            co_await to_clients_.send(pancy::redirection_response{
                pancy::response_header(*req, pancy::errc::redirect), leader_index_
            });
            continue;
        }

        auto* received = std::get_if<std::pair<paxos_message, std::string>>(&msg);
        if (!received) {
            leader_index_ = index_;
            next_round_ = std::max(next_round_, accepted_round_ + 1);
            co_return;
        }

        auto& [paxos_msg, source_id] = *received;
        size_t sender_index = replica_index_from_source_id(source_id);
        unsigned long long incoming_round = message_round(paxos_msg);

        if (incoming_round > accepted_round_) {
            accepted_round_ = incoming_round;
            next_round_ = std::max(next_round_, accepted_round_ + 1);
        }

        auto* propose = std::get_if<propose_msg>(&paxos_msg);
        if (!propose) {
            auto* probe = std::get_if<probe_msg>(&paxos_msg);
            if (!probe)
                continue;

            if (probe->round < accepted_round_) {
                continue;
            }

            prepare_msg prepare;
            prepare.round = probe->round;
            prepare.accepted_round = accepted_round_;
            prepare.applied_up_to = commit_index_;
            prepare.accepted_values = accepted_values_;
            co_await to_replicas_[sender_index]->send(prepare);
            continue;
        }

        leader_index_ = sender_index;

        // Reject stale messages (e.g., old heartbeats arriving after newer proposes)
        if (propose->round < accepted_round_) {
            continue;
        }

        bool no_gap = propose->batch_start <= accepted_values_.size();
        if (no_gap) {
            accepted_values_.resize(propose->batch_start);
            for (const auto& e : propose->entries) {
                accepted_values_.push_back(e);
            }
            accepted_round_ = propose->round;

            for (auto i = commit_index_; i < propose->committed_slot && i < accepted_values_.size(); ++i) {
                db_.process_req(accepted_values_[i]);
            }
            commit_index_ = std::min(propose->committed_slot, (unsigned long long)accepted_values_.size());
        }

        ack_msg ack;
        ack.round = propose->round;
        ack.success = no_gap;
        ack.highest_accepted = accepted_values_.size();
        ack.applied_up_to = commit_index_;
        co_await to_replicas_[leader_index_]->send(ack);
    }
}

// ******** end Pancy service code ********



// Test functions

void set_replica_channel_loss(pt_paxos_instance& inst, size_t replica, double loss) {
    for (size_t i = 0; i < inst.tester.nreplicas; ++i) {
        inst.replicas[replica]->to_replicas_[i]->set_loss(loss);
        inst.replicas[i]->to_replicas_[replica]->set_loss(loss);
    }
    inst.clients.request_channel(replica).set_loss(loss);
    inst.replicas[replica]->to_clients_.set_loss(loss);
}

void set_partition_loss(pt_paxos_instance& inst,
                        const std::vector<size_t>& left,
                        const std::vector<size_t>& right,
                        double loss) {
    for (size_t l : left) {
        for (size_t r : right) {
            inst.replicas[l]->to_replicas_[r]->set_loss(loss);
            inst.replicas[r]->to_replicas_[l]->set_loss(loss);
        }
    }
}

void apply_split_brain(pt_paxos_instance& inst) {
    std::vector<size_t> minority{inst.tester.initial_leader};
    std::vector<size_t> majority;
    majority.reserve(inst.tester.nreplicas - 1);
    for (size_t i = 0; i != inst.tester.nreplicas; ++i) {
        if (i != inst.tester.initial_leader) {
            majority.push_back(i);
        }
    }
    set_partition_loss(inst, minority, majority, 1);
}

cot::task<> fail_primary_after(pt_paxos_instance& inst, cot::duration d) {
    co_await cot::after(d);
    set_replica_channel_loss(inst, inst.tester.initial_leader, 1);
}

cot::task<> up_down_randomly(pt_paxos_instance& inst, int replica, cot::duration d) {
    if (replica >= (int) inst.tester.nreplicas) {
        co_return;
    }
    while (true) {
        co_await cot::after(inst.tester.randomness.uniform(d / 2, d * 3 / 2));
        set_replica_channel_loss(inst, replica, 1);
        co_await cot::after(inst.tester.randomness.uniform(d, d * 3));
        set_replica_channel_loss(inst, replica, inst.tester.loss);
    }
}

cot::task<> clear_after(cot::duration d) {
    co_await cot::after(d);
    cot::clear();
}

bool try_one_seed(testinfo& tester, unsigned long seed) {
    cot::reset();   // clear old events and coroutines
    tester.randomness.seed(seed);

    // Create client generator and test instance
    lockseq_model clients(tester.nreplicas, tester.randomness);
    pt_paxos_instance inst(tester, clients);

    // Start coroutines
    clients.start();
    std::vector<cot::task<>> tasks;
    for (size_t s = 0UL; s != tester.nreplicas; ++s) {
        tasks.push_back(inst.replicas[s]->run());
    }
    cot::task<> timeout_task = clear_after(100s);

    switch (tester.mode) {
        case failure_mode::failed_leader:
            set_replica_channel_loss(inst, tester.initial_leader, 1);
            break;
        case failure_mode::failed_replica:
            set_replica_channel_loss(inst, tester.failed_replica, 1);
            break;
        case failure_mode::multiple_random_up_down:
            tasks.push_back(up_down_randomly(inst, tester.failed_replica, 3s));
            break;
        case failure_mode::split_brain:
            apply_split_brain(inst);
            break;
        case failure_mode::none:
            break;
        case failure_mode::delayed_leader_failure:
            tasks.push_back(fail_primary_after(inst, 10s));
            break;
    }

    // Wait for `timeout_task`
    cot::loop();

    // Check database
    std::print("{} lock, {} write, {} clear, {} unlock\n",
               clients.lock_complete, clients.write_complete,
               clients.clear_complete, clients.unlock_complete);
    size_t reference = tester.initial_leader;
    if (tester.mode == failure_mode::failed_leader
        || tester.mode == failure_mode::split_brain)
        reference = (tester.initial_leader + 1) % tester.nreplicas;
    pancy::pancydb& db = inst.replicas[reference]->db_;

    for (size_t s = 0; s != tester.nreplicas; ++s) {
        if (s == reference)
            continue;
        if (tester.mode == failure_mode::failed_leader && s == tester.initial_leader)
            continue;
        if (tester.mode == failure_mode::failed_replica
            && s == (size_t) tester.failed_replica)
            continue;
        if (tester.mode == failure_mode::delayed_leader_failure && s == tester.initial_leader)
            continue;
        if (tester.mode == failure_mode::split_brain && s == tester.initial_leader)
            continue;
        auto problem = db.diff(inst.replicas[s]->db_, 5);
        if (problem) {
            std::print(std::clog,
                       "*** REPLICA DIVERGENCE on seed {} between replica {} and {} at key {}\n",
                       seed, reference, s, *problem);
            db.print_near(*problem, std::clog);
            inst.replicas[s]->db_.print_near(*problem, std::clog);
            return false;
        }
    }

    if (auto problem = clients.check(db)) {
        std::print(std::clog, "*** FAILURE on seed {} at key {}\n", seed, *problem);
        db.print_near(*problem, std::clog);
        return false;
    } else if (tester.print_db) {
        db.print(std::cout);
    }
    return true;
}


// Argument parsing

static struct option options[] = {
    { "count", required_argument, nullptr, 'n' },
    { "seed", required_argument, nullptr, 'S' },
    { "random-seeds", required_argument, nullptr, 'R' },
    { "loss", required_argument, nullptr, 'l' },
    { "verbose", no_argument, nullptr, 'V' },
    { "print-db", no_argument, nullptr, 'p' },
    { "quiet", no_argument, nullptr, 'q' },
    { "failure-mode", required_argument, nullptr, 'f' },
    { "failed-replica", required_argument, nullptr, 'r' },
    { nullptr, 0, nullptr, 0 }
};

int main(int argc, char* argv[]) {
    testinfo tester;

    std::optional<unsigned long> first_seed;
    unsigned long seed_count = 1;

    auto shortopts = short_options_for(options);
    int ch;
    while ((ch = getopt_long(argc, argv, shortopts.c_str(), options, nullptr)) != -1) {
        if (ch == 'S') {
            first_seed = from_str_chars<unsigned long>(optarg);
        } else if (ch == 'R') {
            seed_count = from_str_chars<unsigned long>(optarg);
        } else if (ch == 'l') {
            tester.loss = from_str_chars<double>(optarg);
        } else if (ch == 'n') {
            tester.nreplicas = from_str_chars<size_t>(optarg);
        } else if (ch == 'V') {
            tester.verbose = true;
        } else if (ch == 'p') {
            tester.print_db = true;
        } else if (ch == 'r') {
            tester.failed_replica = from_str_chars<int>(optarg);
        } else if (ch == 'f') {
            if (strcmp(optarg, "failed_leader") == 0) {
                tester.mode = failure_mode::failed_leader;
            } else if (strcmp(optarg, "failed_replica") == 0) {
                tester.mode = failure_mode::failed_replica;
                if (tester.failed_replica < 0) {
                    std::cerr << "must use -r <int> with failed_replica mode\n";
                    return -1;
                }
            } else if (strcmp(optarg, "multiple_random_up_down") == 0) {
                if (tester.failed_replica < 0) {
                    std::cerr << "must use -r <int> with multiple_random_up_down mode\n";
                    return -1;
                }
                tester.mode = failure_mode::multiple_random_up_down;
            } else if (strcmp(optarg, "split_brain") == 0) {
                tester.mode = failure_mode::split_brain;
            } else if (strcmp(optarg, "delayed_leader_failure") == 0) {
                tester.mode = failure_mode::delayed_leader_failure;
            }
        } else {
            std::print(std::cerr, "Unknown option\n");
            return 1;
        }
    }

    bool ok;
    if (first_seed) {
        ok = try_one_seed(tester, *first_seed);
    } else {
        std::mt19937_64 seed_generator = randomly_seeded<std::mt19937_64>();
        for (unsigned long i = 0; i != seed_count; ++i) {
            if (i > 0 && i % 1000 == 0) {
                std::print(std::cerr, ".");
            }
            unsigned long seed = seed_generator();
            ok = try_one_seed(tester, seed);
            if (!ok) {
                break;
            }
        }
        if (ok && seed_count >= 1000) {
            std::print(std::cerr, "\n");
        }
    }
    return ok ? EXIT_SUCCESS : EXIT_FAILURE;
}
