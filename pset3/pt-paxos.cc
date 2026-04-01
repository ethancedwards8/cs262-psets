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


// Configuration and initialization

pt_paxos_replica::pt_paxos_replica(size_t index, size_t nreplicas, random_source& randomness)
    : index_(index),
      nreplicas_(nreplicas),
      from_clients_(randomness, std::format("R{}", index_)),
      from_replicas_(randomness, std::format("R{}/r", index_)),
      to_clients_(randomness, from_clients_.id()),
      to_replicas_(nreplicas),
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
    probe.leader_id = index_;
    co_await send_to_other_replicas(probe);

    size_t prepare_count = 0;
    unsigned long long highest_accepted_round = accepted_round_;
    std::deque<pancy::request> highest_accepted_values = accepted_values_;
    while (prepare_count < quorum_ - 1) {
        auto received = co_await cot::attempt(
            from_replicas_.receive(),
            cot::after(200ms)
        );

        if (!received) {
            co_await send_to_other_replicas(probe);
            continue;
        }

        auto paxos_msg = std::move(*received);
        auto* prepare = std::get_if<prepare_msg>(&paxos_msg);
        if (!prepare)
            continue;

        if (prepare->round != probe.round)
            continue;

        if (prepare->accepted_round > highest_accepted_round) {
            highest_accepted_round = prepare->accepted_round;
            highest_accepted_values = prepare->accepted_values;
        }
        ++prepare_count;
    }

    if (highest_accepted_round > accepted_round_) {
        accepted_round_ = highest_accepted_round;
        accepted_values_ = highest_accepted_values;
        for (const auto& entry : accepted_values_) {
            db_.process_req(entry);
        }
    }

    while (true) {
        auto received = co_await cot::attempt(
            from_clients_.receive(),
            cot::after(heartbeat_interval_)
        );
        if (!received) {
            propose_msg heartbeat;
            heartbeat.round = next_round_++;
            heartbeat.leader_id = index_;
            co_await send_to_other_replicas(heartbeat);
            continue;
        }

        auto req = std::move(*received);

        propose_msg propose;
        propose.round = next_round_++;
        propose.leader_id = index_;
        propose.entries.push_back(req);
        accepted_round_ = propose.round;
        accepted_values_ = propose.entries;
        co_await send_to_other_replicas(propose);

        size_t ack_count = 0;
        while (ack_count < quorum_ - 1) {
            auto received = co_await cot::attempt(
                from_replicas_.receive(),
                cot::after(200ms)
            );

            if (!received) {
                co_await send_to_other_replicas(propose);
                continue;
            }

            auto paxos_msg = std::move(*received);
            auto* ack = std::get_if<ack_msg>(&paxos_msg);

            if (!ack)
                continue;

            if (ack->round != propose.round || !ack->success)
                continue;

            ++ack_count;
        }

        co_await to_clients_.send(db_.process_req(req));
    }
}

cot::task<> pt_paxos_replica::run_as_follower() {
    while (true) {
        auto msg = co_await cot::first(
            from_clients_.receive(),
            from_replicas_.receive(),
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

        auto* paxos_msg = std::get_if<paxos_message>(&msg);
        if (!paxos_msg) {
            leader_index_ = index_;
            next_round_ = std::max(next_round_, accepted_round_ + 1);
            co_return;
        }

        auto* propose = std::get_if<propose_msg>(paxos_msg);
        if (!propose) {
            auto* probe = std::get_if<probe_msg>(paxos_msg);
            if (!probe)
                continue;

            prepare_msg prepare;
            prepare.round = probe->round;
            prepare.accepted_round = accepted_round_;
            prepare.accepted_values = accepted_values_;
            co_await to_replicas_[probe->leader_id]->send(prepare);
            continue;
        }



        leader_index_ = propose->leader_id;
        if (propose->round > accepted_round_) {
            accepted_round_ = propose->round;
            accepted_values_ = propose->entries;
            for (const auto& entry : propose->entries) {
                db_.process_req(entry);
            }
        }

        ack_msg ack;
        ack.round = propose->round;
        ack.success = propose->round >= accepted_round_;
        ack.highest_accepted = propose->batch_start + propose->entries.size();
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
    pancy::pancydb& db = inst.replicas[tester.initial_leader]->db_;
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
