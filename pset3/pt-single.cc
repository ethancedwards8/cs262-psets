#include "lockseq_model.hh"
#include "pancydb.hh"

namespace cot = cotamer;
using namespace std::chrono_literals;

// testinfo
//    Holds configuration information about this test.

struct testinfo {
    random_source randomness;
    double loss = 0;
    bool verbose = false;
    bool print_db = false;

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


// pt_single_instance
//    Manage a test of a singleton Pancy service.

struct pt_single_instance {
    testinfo& tester_;           // test parameters, including randomness
    client_model& clients_;      // client model (for database testing)

    // ports and channels (here, just one of each)
    netsim::port<pancy::request> in_;
    netsim::channel<pancy::response> out_;

    // underlying database state
    pancy::pancydb db_;

    pt_single_instance(testinfo&, client_model&);
    cot::task<> run();
};

// construct the test instance and connect channels
pt_single_instance::pt_single_instance(testinfo& tester, client_model& clients)
    : tester_(tester),
      clients_(clients),
      in_(tester.randomness, "server"),
      out_(tester.randomness, "server") {
    clients.connect_replica(0, in_, out_);
    tester.configure_port(in_);
    tester.configure_channel(out_);
    tester.configure_quiet_channel(clients_.request_channel(0));
}


// ********** PANCY SERVICE CODE **********

cot::task<> pt_single_instance::run() {
    unsigned long loops = 0;
    while (true) {
        // Obtain request
        auto req = co_await in_.receive();

        // Apply request to database and respond
        co_await out_.send(db_.process_req(std::move(req)));

        // Periodically check database and exit if erroneous
        if (++loops % (1UL << 20) == 0 && clients_.check(db_)) {
            co_return;
        }
    }
}

// ******** end Pancy service code ********


// Test functions

cot::task<> clear_after(cot::duration d) {
    co_await cot::after(d);
    cot::clear();
}

bool try_one_seed(testinfo& tester, unsigned long seed) {
    cot::reset();   // clear old events and coroutines
    tester.randomness.seed(seed);

    // Create client generator and test instance
    lockseq_model clients(1, tester.randomness);
    pt_single_instance inst(tester, clients);

    // Start coroutines
    clients.start();
    cot::task<> run_task = inst.run();
    cot::task<> timeout_task = clear_after(60s);

    // Wait for `timeout_task`
    cot::loop();

    // Check database
    std::print("{} lock, {} write, {} clear, {} unlock\n",
               clients.lock_complete, clients.write_complete,
               clients.clear_complete, clients.unlock_complete);
    if (auto problem = clients.check(inst.db_)) {
        std::print(std::clog, "*** FAILURE on seed {} at key {}\n", seed, *problem);
        inst.db_.print_near(*problem, std::clog);
        return false;
    } else if (tester.print_db) {
        inst.db_.print(std::cout);
    }
    return true;
}


// Argument parsing

static struct option options[] = {
    { "seed", required_argument, nullptr, 'S' },
    { "random-seeds", required_argument, nullptr, 'R' },
    { "loss", required_argument, nullptr, 'l' },
    { "verbose", no_argument, nullptr, 'V' },
    { "print-db", no_argument, nullptr, 'p' },
    { "quiet", no_argument, nullptr, 'q' },
    { nullptr, 0, nullptr, 0 }
};

int main(int argc, char* argv[]) {
    testinfo tester;

    // Read program options: `-S SEED` sets the desired random seed, `-l LOSS`
    // sets the channel loss probability, and `-R COUNT` runs COUNT times with
    // different random seeds, exiting on the first problem. Add more options
    // by extending the `options` and `testinfo` structures. (For instance,
    // `pt-paxos.cc` adds `-n N` to set the number of replicas.)
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
        } else if (ch == 'V') {
            tester.verbose = true;
        } else if (ch == 'p') {
            tester.print_db = true;
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
