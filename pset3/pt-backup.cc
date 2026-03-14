#include "lockseq_model.hh"
#include "pancydb.hh"
#include "netsim.hh"

namespace cot = cotamer;
using namespace std::chrono_literals;

// testinfo
//    Holds configuration information about this test.

struct testinfo {
    random_source randomness;
    double loss = 0.0;           // channel loss rate
    bool verbose = false;        // whether to print messages
    bool print_db = false;       // print database on success
    bool bogus = false;          // implement erroneous protocol

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


// pt_backup_instance
//    Manage a test of a primary-backup Pancy service.

struct pt_backup_instance {
    testinfo& tester_;
    client_model& clients_;

    // primary-to-backup communication
    netsim::port<std::pair<uint64_t, pancy::request>> p2b_port_;
    netsim::channel<std::pair<uint64_t, pancy::request>> p2b_chan_;

    // backup-to-primary communication
    netsim::port<uint64_t> b2p_port_;
    netsim::channel<uint64_t> b2p_chan_;

    // client-primary communication
    netsim::port<pancy::request> c2p_port_;
    netsim::channel<pancy::response> p2c_chan_;

    // client-backup communication
    netsim::port<pancy::request> c2b_port_;
    netsim::channel<pancy::response> b2c_chan_;

    // primary and backup databases
    pancy::pancydb primarydb_;
    pancy::pancydb backupdb_;
    bool primarydb_error_ = false;

    pt_backup_instance(testinfo&, client_model&);
    cot::task<> primary();
    cot::task<> backup();
};

// Configuration and initialization

pt_backup_instance::pt_backup_instance(testinfo& tester, client_model& clients)
    : tester_(tester),
      clients_(clients),
      p2b_port_(tester_.randomness, "backup/r"),
      p2b_chan_(tester_.randomness, "primary"),
      b2p_port_(tester_.randomness, "primary/r"),
      b2p_chan_(tester_.randomness, "backup"),
      c2p_port_(tester_.randomness, "primary"),
      p2c_chan_(tester_.randomness, "primary"),
      c2b_port_(tester_.randomness, "backup"),
      b2c_chan_(tester_.randomness, "backup") {
    p2b_chan_.connect(p2b_port_);
    b2p_chan_.connect(b2p_port_);
    clients_.connect_replica(0, c2p_port_, p2c_chan_);
    clients_.connect_replica(1, c2b_port_, b2c_chan_);
    tester_.configure_port(p2b_port_);
    tester_.configure_port(b2p_port_);
    tester_.configure_port(c2p_port_);
    tester_.configure_port(c2b_port_);
    tester_.configure_channel(p2b_chan_);
    tester_.configure_channel(p2c_chan_);
    tester_.configure_channel(b2p_chan_);
    tester_.configure_channel(b2c_chan_);
    tester_.configure_quiet_channel(clients_.request_channel(0));
    tester_.configure_quiet_channel(clients_.request_channel(1));
}



// ********** PANCY SERVICE CODE **********

cot::task<> pt_backup_instance::primary() {
    uint64_t serial = 0;
    unsigned long loops = 0;
    while (true) {
        // Obtain request
        auto req = co_await c2p_port_.receive();

        // Pass `-B/--bogus` to trigger this ERRONEOUS code.
        if (tester_.bogus) {
            // Process request (ERRONEOUSLY!)
            co_await p2c_chan_.send(primarydb_.process_req(req));
        }

        // Forward request to backup, wait for response
        ++serial;
        while (true) {
            co_await p2b_chan_.send(std::make_pair(serial, req));
            auto ret = co_await cot::attempt(
                b2p_port_.receive(),
                cot::after(1s)
            );
            if (ret && *ret == serial) {
                break;
            }
        }

        if (!tester_.bogus) {
            // Apply request to database and respond
            co_await p2c_chan_.send(primarydb_.process_req(std::move(req)));
        }

        // Periodically check database and exit if erroneous
        if ((++loops % (1UL << 20)) == 0 && clients_.check(primarydb_)) {
            primarydb_error_ = true;
            co_return;
        }
    }
}

cot::task<> pt_backup_instance::backup() {
    // Operate in backup mode until timeout
    uint64_t expected_serial = 0;
    while (true) {
        // receive request from primary, client, or timeout
        auto ret = co_await cot::first(
            p2b_port_.receive(),
            c2b_port_.receive(),
            cot::after(5s)
        );
        if (ret.index() == 2) {
            // timeout
            break;
        }
        if (ret.index() == 1) {
            // redirect client to leader
            const auto& cmsg = std::get<1>(ret);
            co_await b2c_chan_.send(pancy::redirection_response{
                pancy::response_header(cmsg, pancy::errc::redirect), 0
            });
            continue;
        }

        // apply request to database
        const auto& pmsg = std::get<0>(ret);
        if (pmsg.first == expected_serial + 1) {
            ++expected_serial;
            backupdb_.process_req(pmsg.second);
        } else {
            assert(pmsg.first == expected_serial);
        }

        // acknowledge request
        co_await b2p_chan_.send(expected_serial);
    }

    // If we get here, the primary has failed; it's our turn to process
    // requests.

    unsigned long loops = 0;
    while (true) {
        // Receive request, process and respond
        auto req = co_await c2b_port_.receive();
        co_await b2c_chan_.send(backupdb_.process_req(req));

        if ((++loops % (1UL << 20)) == 0 && clients_.check(backupdb_)) {
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

cot::task<> fail_primary_after(pt_backup_instance& inst, cot::duration d) {
    co_await cot::after(d);
    inst.p2b_chan_.set_loss(1.0);
    inst.p2c_chan_.set_loss(1.0);
}

bool try_one_seed(testinfo& tester, unsigned long seed) {
    cot::reset();   // clear old events and coroutines
    tester.randomness.seed(seed);

    // Create client generator and test instance
    lockseq_model clients(2, tester.randomness);
    pt_backup_instance inst(tester, clients);

    // Start coroutines
    clients.start();
    cot::task<> primary_task = inst.primary();
    cot::task<> backup_task = inst.backup();
    cot::task<> failure_task = fail_primary_after(inst, 60s);
    cot::task<> timeout_task = clear_after(100s);

    // Wait for `timeout_task`
    cot::loop();

    // Check database
    std::print("{} lock, {} write, {} clear, {} unlock\n",
               clients.lock_complete, clients.write_complete,
               clients.clear_complete, clients.unlock_complete);
    pancy::pancydb& db = inst.primarydb_error_ ? inst.primarydb_ : inst.backupdb_;
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
    { "seed", required_argument, nullptr, 'S' },
    { "random-seeds", required_argument, nullptr, 'R' },
    { "loss", required_argument, nullptr, 'l' },
    { "bogus", no_argument, nullptr, 'B' },
    { "verbose", no_argument, nullptr, 'V' },
    { "print-db", no_argument, nullptr, 'p' },
    { "quiet", no_argument, nullptr, 'q' },
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
        } else if (ch == 'V') {
            tester.verbose = true;
        } else if (ch == 'B') {
            tester.bogus = true;
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
