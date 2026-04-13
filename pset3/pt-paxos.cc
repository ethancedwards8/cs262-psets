#include "lockseq_model.hh"
#include <algorithm>
#include <cassert>
#include <cstring>
#include <optional>
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
    unstable_leader_mixed,
    random_failure_schedule,
    william_link_schedule,
    disruptive_isolate,
    split_brain,
    minority_partition_failure,
    vihaan_split_brain_failure,
    delayed_leader_failure,
    cascading_star_partition,
    split_brain_isolate_heal,
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
    std::vector<size_t> excluded_replicas;

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
enum class raft_role {
    follower,
    candidate,
    leader,
};

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
    random_source& randomness_;
    raft_role role_ = raft_role::follower;
    unsigned long long current_term_ = 0;
    std::optional<size_t> voted_for_;
    std::deque<log_entry> log_;
    unsigned long long commit_index_ = 0;
    unsigned long long applied_index_ = 0;
    std::vector<unsigned long long> next_index_;
    std::vector<unsigned long long> match_index_;
    std::deque<unsigned long long> pending_client_slots_;
    cot::duration heartbeat_interval_ = 50ms;
    cot::duration election_timeout_;

    pt_paxos_replica(size_t index, size_t nreplicas, random_source&);
    void initialize(pt_paxos_instance&);

    cot::task<> run();
    cot::task<> run_as_leader();
    cot::task<> run_as_follower();
    cot::task<> run_as_candidate();
    cot::task<> send_to_other_replicas(const paxos_message& msg);

private: 
    unsigned long quorum_ = nreplicas_ / 2 + 1;
    void reset_election_timeout();
    void become_follower(unsigned long long term, std::optional<size_t> leader = std::nullopt);
    void become_leader();
    unsigned long long last_log_index() const;
    unsigned long long last_log_term() const;
    unsigned long long log_term(unsigned long long index) const;
    bool candidate_log_is_up_to_date(const request_vote_request& vote) const;
    append_entries_request make_append_entries(size_t follower) const;
    cot::task<> apply_committed_entries();
    cot::task<bool> handle_append_entries(const append_entries_request& append);
    cot::task<bool> handle_request_vote(const request_vote_request& vote, size_t sender_index);
    cot::task<> send_append_entries(size_t follower);
    void advance_commit_index();
    void check_paxos_invariants() const;
};

struct pt_paxos_instance {
    testinfo& tester;
    client_model& clients;
    std::vector<std::unique_ptr<pt_paxos_replica>> replicas;
    // ...plus anything you want to add

    pt_paxos_instance(testinfo&, client_model&);
};

cot::task<> up_down_randomly(pt_paxos_instance& inst, int replica, cot::duration d);
cot::task<> partition_groups_forever(pt_paxos_instance& inst,
                                     std::vector<size_t> left,
                                     std::vector<size_t> right);

static size_t replica_index_from_source_id(const std::string& source_id) {
    return from_str_chars<size_t>(source_id.substr(1));
}

static unsigned long long message_term(const paxos_message& msg) {
    return std::visit([](const auto& m) {
        if constexpr (requires { m.term; }) {
            return m.term;
        } else {
            return m.round;
        }
    }, msg);
}

void pt_paxos_replica::check_paxos_invariants() const {
    assert(log_.size() >= commit_index_);
    assert(applied_index_ <= commit_index_);
}


// Configuration and initialization

pt_paxos_replica::pt_paxos_replica(size_t index, size_t nreplicas, random_source& randomness)
    : index_(index),
      nreplicas_(nreplicas),
      from_clients_(randomness, std::format("R{}", index_)),
      from_replicas_(randomness, std::format("R{}/r", index_)),
      to_clients_(randomness, from_clients_.id()),
      to_replicas_(nreplicas),
      randomness_(randomness),
      next_index_(nreplicas, 1),
      match_index_(nreplicas, 0),
      election_timeout_(randomness.uniform(800ms, 1200ms)) {
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
        if (role_ == raft_role::leader) {
            co_await run_as_leader();
        } else if (role_ == raft_role::candidate) {
            co_await run_as_candidate();
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

void pt_paxos_replica::reset_election_timeout() {
    election_timeout_ = randomness_.uniform(800ms, 1200ms);
}

void pt_paxos_replica::become_follower(unsigned long long term,
                                       std::optional<size_t> leader) {
    if (term > current_term_) {
        current_term_ = term;
        voted_for_.reset();
    }
    role_ = raft_role::follower;
    pending_client_slots_.clear();
    if (leader) {
        leader_index_ = *leader;
    }
    reset_election_timeout();
}

void pt_paxos_replica::become_leader() {
    role_ = raft_role::leader;
    leader_index_ = index_;
    std::fill(next_index_.begin(), next_index_.end(), last_log_index() + 1);
    std::fill(match_index_.begin(), match_index_.end(), 0);
    match_index_[index_] = last_log_index();
    pending_client_slots_.clear();
}

unsigned long long pt_paxos_replica::last_log_index() const {
    return log_.size();
}

unsigned long long pt_paxos_replica::last_log_term() const {
    return log_.empty() ? 0 : log_.back().term;
}

unsigned long long pt_paxos_replica::log_term(unsigned long long index) const {
    assert(index <= log_.size());
    return index == 0 ? 0 : log_[index - 1].term;
}

bool pt_paxos_replica::candidate_log_is_up_to_date(const request_vote_request& vote) const {
    auto our_last_term = last_log_term();
    if (vote.last_log_term != our_last_term) {
        return vote.last_log_term > our_last_term;
    }
    return vote.last_log_index >= last_log_index();
}

append_entries_request pt_paxos_replica::make_append_entries(size_t follower) const {
    append_entries_request append;
    append.term = current_term_;
    append.leader_id = index_;
    append.leader_commit = commit_index_;
    auto next = std::clamp<unsigned long long>(
        next_index_[follower], 1, last_log_index() + 1
    );
    append.prev_log_index = next - 1;
    append.prev_log_term = log_term(append.prev_log_index);
    for (auto i = next; i <= last_log_index(); ++i) {
        append.entries.push_back(log_[i - 1]);
    }
    return append;
}

cot::task<> pt_paxos_replica::apply_committed_entries() {
    while (applied_index_ < commit_index_) {
        ++applied_index_;
        auto response = db_.process_req(log_[applied_index_ - 1].command);
        if (!pending_client_slots_.empty()
            && pending_client_slots_.front() == applied_index_) {
            co_await to_clients_.send(std::move(response));
            pending_client_slots_.pop_front();
        }
    }
    check_paxos_invariants();
}

cot::task<bool> pt_paxos_replica::handle_request_vote(const request_vote_request& vote,
                                                      size_t sender_index) {
    request_vote_response response;

    if (vote.term < current_term_) {
        response.term = current_term_;
        response.vote_granted = false;
        co_await to_replicas_[sender_index]->send(response);
        co_return false;
    }

    if (vote.term > current_term_) {
        become_follower(vote.term);
    }

    bool can_vote = !voted_for_ || *voted_for_ == vote.candidate_id;
    bool grant = can_vote && candidate_log_is_up_to_date(vote);
    if (grant) {
        voted_for_ = vote.candidate_id;
        role_ = raft_role::follower;
        reset_election_timeout();
    }

    response.term = current_term_;
    response.vote_granted = grant;
    co_await to_replicas_[sender_index]->send(response);
    co_return grant;
}

cot::task<bool> pt_paxos_replica::handle_append_entries(const append_entries_request& append) {
    append_entries_response response;
    response.term = current_term_;

    if (append.term < current_term_) {
        co_await to_replicas_[append.leader_id]->send(response);
        co_return false;
    }

    if (append.term > current_term_ || role_ != raft_role::follower) {
        become_follower(append.term, append.leader_id);
    } else {
        leader_index_ = append.leader_id;
        reset_election_timeout();
    }
    response.term = current_term_;

    if (append.prev_log_index > last_log_index()) {
        response.conflict_index = last_log_index() + 1;
        response.match_index = last_log_index();
        co_await to_replicas_[append.leader_id]->send(response);
        co_return true;
    }

    if (log_term(append.prev_log_index) != append.prev_log_term) {
        response.conflict_term = log_term(append.prev_log_index);
        response.conflict_index = append.prev_log_index;
        while (response.conflict_index > 1
               && log_term(response.conflict_index - 1) == response.conflict_term) {
            --response.conflict_index;
        }
        response.match_index = response.conflict_index - 1;
        co_await to_replicas_[append.leader_id]->send(response);
        co_return true;
    }

    unsigned long long insert_index = append.prev_log_index + 1;
    size_t entry_offset = 0;
    while (entry_offset < append.entries.size()
           && insert_index + entry_offset <= last_log_index()
           && log_term(insert_index + entry_offset) == append.entries[entry_offset].term) {
        ++entry_offset;
    }

    if (entry_offset < append.entries.size()) {
        log_.resize(insert_index + entry_offset - 1);
        while (entry_offset < append.entries.size()) {
            log_.push_back(append.entries[entry_offset]);
            ++entry_offset;
        }
    }

    if (append.leader_commit > commit_index_) {
        commit_index_ = std::min(append.leader_commit, last_log_index());
        co_await apply_committed_entries();
    }

    response.success = true;
    response.match_index = append.prev_log_index + append.entries.size();
    co_await to_replicas_[append.leader_id]->send(response);
    co_return true;
}

cot::task<> pt_paxos_replica::send_append_entries(size_t follower) {
    if (follower == index_)
        co_return;
    co_await to_replicas_[follower]->send(make_append_entries(follower));
}

void pt_paxos_replica::advance_commit_index() {
    match_index_[index_] = last_log_index();
    for (auto n = last_log_index(); n > commit_index_; --n) {
        if (log_term(n) != current_term_) {
            continue;
        }
        size_t replicated = 1; // leader
        for (size_t s = 0; s != nreplicas_; ++s) {
            if (s != index_ && match_index_[s] >= n) {
                ++replicated;
            }
        }
        if (replicated >= quorum_) {
            commit_index_ = n;
            return;
        }
    }
}

cot::task<> pt_paxos_replica::run_as_leader() {
    check_paxos_invariants();

    for (size_t s = 0; s != nreplicas_; ++s) {
        co_await send_append_entries(s);
    }

    while (true) {
        auto event = co_await cot::first(
            from_clients_.receive(),
            from_replicas_.receive_with_id(),
            cot::after(heartbeat_interval_)
        );

        auto* req = std::get_if<pancy::request>(&event);
        if (req) {
            log_.push_back(log_entry{current_term_, std::move(*req)});
            match_index_[index_] = last_log_index();
            pending_client_slots_.push_back(last_log_index());
            check_paxos_invariants();
            advance_commit_index();
            co_await apply_committed_entries();
            for (size_t s = 0; s != nreplicas_; ++s) {
                co_await send_append_entries(s);
            }
            continue;
        }

        auto* received = std::get_if<std::pair<paxos_message, std::string>>(&event);
        if (received) {
            auto& [paxos_msg, source_id] = *received;
            size_t sender_index = replica_index_from_source_id(source_id);

            if (auto* append = std::get_if<append_entries_request>(&paxos_msg)) {
                if (append->term < current_term_) {
                    co_await handle_append_entries(*append);
                } else if (append->leader_id != index_) {
                    co_await handle_append_entries(*append);
                    co_return;
                }
                continue;
            }

            if (auto* vote = std::get_if<request_vote_request>(&paxos_msg)) {
                if (vote->term > current_term_) {
                    co_await handle_request_vote(*vote, sender_index);
                    co_return;
                }
                request_vote_response response;
                response.term = current_term_;
                co_await to_replicas_[sender_index]->send(response);
                continue;
            }

            if (message_term(paxos_msg) > current_term_) {
                become_follower(message_term(paxos_msg), sender_index);
                co_return;
            }

            auto* append_response = std::get_if<append_entries_response>(&paxos_msg);
            if (!append_response || append_response->term != current_term_) {
                continue;
            }

            if (append_response->success) {
                match_index_[sender_index] = std::max(match_index_[sender_index],
                                                      append_response->match_index);
                next_index_[sender_index] = std::max(next_index_[sender_index],
                                                     match_index_[sender_index] + 1);
                advance_commit_index();
                co_await apply_committed_entries();
            } else if (append_response->conflict_term != 0) {
                auto next_index = append_response->conflict_index;
                for (auto i = last_log_index(); i >= 1; --i) {
                    if (log_term(i) == append_response->conflict_term) {
                        next_index = i + 1;
                        break;
                    }
                    if (i == 1)
                        break;
                }
                next_index_[sender_index] = std::max<unsigned long long>(1, next_index);
                co_await send_append_entries(sender_index);
            } else {
                next_index_[sender_index] = std::max<unsigned long long>(
                    1, append_response->conflict_index
                );
                co_await send_append_entries(sender_index);
            }
            continue;
        }

        for (size_t s = 0; s != nreplicas_; ++s) {
            co_await send_append_entries(s);
        }
    }
}

cot::task<> pt_paxos_replica::run_as_follower() {
    check_paxos_invariants();
    reset_election_timeout();
    auto election_deadline = cot::steady_now() + election_timeout_;

    while (role_ == raft_role::follower) {
        auto remaining = election_deadline - cot::steady_now();
        if (remaining <= cot::duration::zero()) {
            role_ = raft_role::candidate;
            co_return;
        }

        auto event = co_await cot::first(
            from_clients_.receive(),
            from_replicas_.receive_with_id(),
            cot::after(remaining)
        );

        auto* req = std::get_if<pancy::request>(&event);
        if (req) {
            co_await cot::after(.02s); // to make it more obvious in the viz
            co_await to_clients_.send(pancy::redirection_response{
                pancy::response_header(*req, pancy::errc::redirect), leader_index_
            });
            continue;
        }

        auto* received = std::get_if<std::pair<paxos_message, std::string>>(&event);
        if (!received) {
            role_ = raft_role::candidate;
            co_return;
        }

        auto& [paxos_msg, source_id] = *received;
        size_t sender_index = replica_index_from_source_id(source_id);
        bool reset_timer = false;

        if (auto* append = std::get_if<append_entries_request>(&paxos_msg)) {
            reset_timer = co_await handle_append_entries(*append);
        } else if (auto* vote = std::get_if<request_vote_request>(&paxos_msg)) {
            reset_timer = co_await handle_request_vote(*vote, sender_index);
        } else if (message_term(paxos_msg) > current_term_) {
            become_follower(message_term(paxos_msg), sender_index);
        }

        if (reset_timer) {
            election_deadline = cot::steady_now() + election_timeout_;
        }
    }
}

cot::task<> pt_paxos_replica::run_as_candidate() {
    ++current_term_;
    voted_for_ = index_;
    leader_index_ = index_;
    reset_election_timeout();

    request_vote_request vote;
    vote.term = current_term_;
    vote.candidate_id = index_;
    vote.last_log_index = last_log_index();
    vote.last_log_term = last_log_term();
    co_await send_to_other_replicas(vote);

    std::vector<bool> votes(nreplicas_, false);
    votes[index_] = true;
    size_t vote_count = 1;
    auto election_deadline = cot::steady_now() + election_timeout_;

    while (role_ == raft_role::candidate) {
        if (vote_count >= quorum_) {
            become_leader();
            co_return;
        }

        auto remaining = election_deadline - cot::steady_now();
        if (remaining <= cot::duration::zero()) {
            co_return;
        }

        auto event = co_await cot::first(
            from_clients_.receive(),
            from_replicas_.receive_with_id(),
            cot::after(remaining)
        );

        if (auto* req = std::get_if<pancy::request>(&event)) {
            co_await cot::after(.02s);
            co_await to_clients_.send(pancy::redirection_response{
                pancy::response_header(*req, pancy::errc::redirect), leader_index_
            });
            continue;
        }

        auto* received = std::get_if<std::pair<paxos_message, std::string>>(&event);
        if (!received) {
            co_return;
        }

        auto& [paxos_msg, source_id] = *received;
        size_t sender_index = replica_index_from_source_id(source_id);

        if (auto* append = std::get_if<append_entries_request>(&paxos_msg)) {
            if (append->term < current_term_) {
                co_await handle_append_entries(*append);
            } else {
                co_await handle_append_entries(*append);
                co_return;
            }
            continue;
        }

        if (auto* vote_request = std::get_if<request_vote_request>(&paxos_msg)) {
            bool reset_timer = co_await handle_request_vote(*vote_request, sender_index);
            if (role_ != raft_role::candidate)
                co_return;
            if (reset_timer) {
                election_deadline = cot::steady_now() + election_timeout_;
            }
            continue;
        }

        if (message_term(paxos_msg) > current_term_) {
            become_follower(message_term(paxos_msg), sender_index);
            co_return;
        }

        auto* vote_response = std::get_if<request_vote_response>(&paxos_msg);
        if (!vote_response || vote_response->term != current_term_
            || !vote_response->vote_granted || votes[sender_index]) {
            continue;
        }

        votes[sender_index] = true;
        ++vote_count;
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

void fail(pt_paxos_instance& inst, size_t a, size_t b) {
    inst.replicas[a]->to_replicas_[b]->set_loss(1.0);
    inst.replicas[b]->to_replicas_[a]->set_loss(1.0);
}

void recover(pt_paxos_instance& inst, size_t a, size_t b, double loss) {
    inst.replicas[a]->to_replicas_[b]->set_loss(loss);
    inst.replicas[b]->to_replicas_[a]->set_loss(loss);
}

void fail_replica(pt_paxos_instance& inst, size_t idx) {
    size_t n = inst.replicas.size();
    for (size_t i = 0; i < n; ++i) {
        inst.replicas[idx]->to_replicas_[i]->set_loss(1.0);
        inst.replicas[i]->to_replicas_[idx]->set_loss(1.0);
    }
    inst.clients.request_channel(idx).set_loss(1.0);
    inst.replicas[idx]->to_clients_.set_loss(1.0);
}

void recover_replica(pt_paxos_instance& inst, size_t idx, double base_loss) {
    size_t n = inst.replicas.size();
    for (size_t i = 0; i < n; ++i) {
        inst.replicas[idx]->to_replicas_[i]->set_loss(base_loss);
        inst.replicas[i]->to_replicas_[idx]->set_loss(base_loss);
    }
    inst.clients.request_channel(idx).set_loss(base_loss);
    inst.replicas[idx]->to_clients_.set_loss(base_loss);
}

inline void set_replica_link_loss(pt_paxos_instance& inst, size_t i, size_t j, double loss) {
    if (i == j)
        return;
    inst.replicas[i]->to_replicas_[j]->set_loss(loss);
}

inline void set_within_side_links(pt_paxos_instance& inst, size_t left_size, double loss) {
    const size_t n = inst.replicas.size();
    for (size_t i = 0; i < n; ++i) {
        for (size_t j = 0; j < n; ++j) {
            if (i == j)
                continue;
            const bool same_side = (i < left_size) == (j < left_size);
            if (same_side)
                set_replica_link_loss(inst, i, j, loss);
        }
    }
}

inline cot::task<> heal_all_directed_edges_one_by_one(pt_paxos_instance& inst,
                                                      testinfo& tester,
                                                      cot::duration step_pause) {
    const size_t n = inst.replicas.size();
    for (size_t i = 0; i < n; ++i) {
        for (size_t j = 0; j < n; ++j) {
            if (i == j)
                continue;
            set_replica_link_loss(inst, i, j, tester.loss);
            co_await cot::after(step_pause);
        }
    }
}

cot::task<> recover_replica_after(pt_paxos_instance& inst,
                                  testinfo& tester,
                                  size_t replica,
                                  cot::duration delay,
                                  std::shared_ptr<std::vector<bool>> active_failures,
                                  std::shared_ptr<size_t> currently_failed) {
    co_await cot::after(delay);
    if (replica >= tester.nreplicas || !(*active_failures)[replica])
        co_return;
    set_replica_channel_loss(inst, replica, tester.loss);
    (*active_failures)[replica] = false;
    if (*currently_failed > 0)
        --*currently_failed;
}

cot::task<> partition_replicas_after(pt_paxos_instance& inst,
                                     testinfo& tester,
                                     size_t a,
                                     size_t b,
                                     cot::duration delay,
                                     cot::duration heal_after = 0ms,
                                     bool two_way = true) {
    co_await cot::after(delay);
    set_replica_link_loss(inst, a, b, 1.0);
    if (two_way)
        set_replica_link_loss(inst, b, a, 1.0);

    if (heal_after <= 0ms)
        co_return;

    co_await cot::after(heal_after);
    set_replica_link_loss(inst, a, b, tester.loss);
    if (two_way)
        set_replica_link_loss(inst, b, a, tester.loss);
}

void set_replica_to_replica_loss(pt_paxos_instance& inst, double loss) {
    for (size_t from = 0; from != inst.tester.nreplicas; ++from) {
        for (size_t to = 0; to != inst.tester.nreplicas; ++to) {
            if (from == to) {
                continue;
            }
            inst.replicas[from]->to_replicas_[to]->set_loss(loss);
        }
    }
}

void apply_star_partition(pt_paxos_instance& inst, const std::vector<size_t>& hubs) {
    std::vector<bool> is_hub(inst.tester.nreplicas, false);
    for (size_t hub : hubs) {
        if (hub < inst.tester.nreplicas) {
            is_hub[hub] = true;
        }
    }

    set_replica_to_replica_loss(inst, 1);
    for (size_t from = 0; from != inst.tester.nreplicas; ++from) {
        for (size_t to = 0; to != inst.tester.nreplicas; ++to) {
            if (from == to) {
                continue;
            }
            if (is_hub[from] || is_hub[to]) {
                inst.replicas[from]->to_replicas_[to]->set_loss(inst.tester.loss);
            }
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

cot::task<> minority_partition(pt_paxos_instance& inst, size_t leader_id, cot::duration duration) {
    co_await cot::after(10s);

    size_t num_replicas = inst.replicas.size();
    size_t majority = num_replicas / 2 + 1;

    std::vector<size_t> majority_set;
    std::vector<size_t> minority_set{leader_id};

    for (size_t i = 0; i < num_replicas; ++i) {
        if (i == leader_id) {
            continue;
        }
        if (majority_set.size() < majority) {
            majority_set.push_back(i);
        } else {
            minority_set.push_back(i);
        }
    }

    for (size_t a : majority_set) {
        for (size_t b : minority_set) {
            fail(inst, a, b);
            inst.replicas[b]->to_clients_.set_loss(1.0);
            inst.clients.request_channel(b).set_loss(1.0);
        }
    }

    co_await cot::after(duration);

    double loss = inst.tester.loss;
    for (size_t a : majority_set) {
        for (size_t b : minority_set) {
            recover(inst, a, b, loss);
            inst.replicas[b]->to_clients_.set_loss(loss);
            inst.clients.request_channel(b).set_loss(loss);
        }
    }
}

cot::task<> fail_primary_after(pt_paxos_instance& inst, cot::duration d) {
    co_await cot::after(d);
    set_replica_channel_loss(inst, inst.tester.initial_leader, 1);
}

cot::task<> cascading_star_partition_scenario(pt_paxos_instance& inst) {
    if (inst.tester.nreplicas < 3) {
        co_return;
    }

    std::vector<size_t> hubs{inst.tester.initial_leader};
    apply_star_partition(inst, hubs);

    std::vector<size_t> candidates;
    candidates.reserve(inst.tester.nreplicas - 1);
    for (size_t i = 0; i != inst.tester.nreplicas; ++i) {
        if (i != inst.tester.initial_leader) {
            candidates.push_back(i);
        }
    }
    if (candidates.size() < 2) {
        co_return;
    }

    std::shuffle(candidates.begin(), candidates.end(), inst.tester.randomness.engine());
    size_t first_successor = candidates[0];
    size_t second_successor = candidates[1];
    inst.tester.excluded_replicas = {
        inst.tester.initial_leader,
        first_successor,
        second_successor
    };

    co_await cot::after(30s);
    set_replica_channel_loss(inst, inst.tester.initial_leader, 1);

    co_await cot::after(5s);
    hubs.push_back(first_successor);
    apply_star_partition(inst, hubs);
    cot::task<> first_flap = up_down_randomly(inst, static_cast<int>(first_successor), 3s);

    co_await cot::after(10s);
    hubs.push_back(second_successor);
    apply_star_partition(inst, hubs);

    co_await cot::after(10s);
    cot::task<> second_flap = up_down_randomly(inst, static_cast<int>(second_successor), 3s);

    co_await cot::after(1h);
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

cot::task<> partition_groups_forever(pt_paxos_instance& inst,
                                     std::vector<size_t> left,
                                     std::vector<size_t> right) {
    set_partition_loss(inst, left, right, 1);
    co_await cot::after(1h);
}

cot::task<> failure_split_brain(pt_paxos_instance& inst,
                                cot::duration after,
                                cot::duration duration) {
    co_await cot::after(after);

    size_t n = inst.replicas.size();
    size_t mid = n / 2;

    for (size_t i = 0; i < mid; ++i) {
        for (size_t j = mid; j < n; ++j) {
            inst.replicas[i]->to_replicas_[j]->set_loss(1.0);
            inst.replicas[j]->to_replicas_[i]->set_loss(1.0);
        }
    }

    co_await cot::after(duration);

    double base_loss = inst.tester.loss;
    for (size_t i = 0; i < mid; ++i) {
        for (size_t j = mid; j < n; ++j) {
            inst.replicas[i]->to_replicas_[j]->set_loss(base_loss);
            inst.replicas[j]->to_replicas_[i]->set_loss(base_loss);
        }
    }
}

cot::task<> split_brain_isolate_heal_schedule(pt_paxos_instance& inst,
                                              testinfo& tester,
                                              cot::duration t_split = 10s,
                                              cot::duration t_isolate_within = 10s,
                                              cot::duration step_pause = 1s,
                                              size_t left_size = 0) {
    const size_t n = tester.nreplicas;
    if (left_size == 0)
        left_size = n / 2;
    if (n <= 1 || left_size == 0 || left_size >= n)
        co_return;

    std::vector<size_t> left;
    std::vector<size_t> right;
    left.reserve(left_size);
    right.reserve(n - left_size);
    for (size_t i = 0; i < n; ++i) {
        if (i < left_size)
            left.push_back(i);
        else
            right.push_back(i);
    }

    co_await cot::after(t_split);
    set_partition_loss(inst, left, right, 1.0);

    co_await cot::after(t_isolate_within);
    set_within_side_links(inst, left_size, 1.0);

    co_await heal_all_directed_edges_one_by_one(inst, tester, step_pause);
}

cot::task<> random_failure_schedule_task(pt_paxos_instance& inst) {
    size_t n = inst.tester.nreplicas;
    size_t max_failed = (n - 1) / 2;
    auto active_failures = std::make_shared<std::vector<bool>>(n, false);
    auto currently_failed = std::make_shared<size_t>(0);
    std::vector<cot::task<>> scheduled_events;
    random_source& rng = inst.tester.randomness;

    while (true) {
        co_await cot::after(rng.uniform(200ms, 800ms));

        double roll = rng.uniform(0.0, 1.0);
        if (roll < 0.4 && *currently_failed < max_failed) {
            std::vector<size_t> candidates;
            candidates.reserve(n);
            for (size_t i = 0; i != n; ++i) {
                if (!(*active_failures)[i])
                    candidates.push_back(i);
            }
            if (candidates.empty())
                continue;

            size_t victim = candidates[rng.uniform<size_t>(0, candidates.size() - 1)];
            bool recovers = rng.coin_flip();
            cot::duration crash_dur = recovers ? rng.uniform(100ms, 500ms) : 0ms;

            (*active_failures)[victim] = true;
            ++*currently_failed;
            set_replica_channel_loss(inst, victim, 1.0);

            if (crash_dur > 0ms) {
                scheduled_events.push_back(recover_replica_after(
                    inst, inst.tester, victim, crash_dur, active_failures, currently_failed
                ));
            } else if (std::find(inst.tester.excluded_replicas.begin(),
                                 inst.tester.excluded_replicas.end(),
                                 victim) == inst.tester.excluded_replicas.end()) {
                inst.tester.excluded_replicas.push_back(victim);
            }
        } else if (roll < 0.7) {
            if (n < 2)
                continue;
            size_t a = rng.uniform<size_t>(0, n - 1);
            size_t b = a;
            while (b == a)
                b = rng.uniform<size_t>(0, n - 1);

            bool two_way = rng.coin_flip();
            bool heals = rng.coin_flip();
            cot::duration heal_after = heals ? rng.uniform(100ms, 400ms) : 0ms;

            scheduled_events.push_back(partition_replicas_after(
                inst, inst.tester, a, b, 0ms, heal_after, two_way
            ));
        }
    }
}

cot::task<> random_link_schedule(pt_paxos_instance& inst, testinfo& tester) {
    size_t n = tester.nreplicas;
    if (n < 2)
        co_return;
    double base_loss = tester.loss / 5.0;

    std::vector<std::vector<bool>> failed(n, std::vector<bool>(n, false));
    size_t num_failed_links = 0;
    size_t max_failed_links = n;

    while (true) {
        co_await cot::after(tester.randomness.uniform(5s, 15s));

        if (num_failed_links < max_failed_links && tester.randomness.coin_flip(0.6)) {
            size_t i = tester.randomness.uniform(size_t(0), n - 1);
            size_t j = (i + 1 + tester.randomness.uniform(size_t(0), n - 2)) % n;
            if (!failed[i][j]) {
                fail(inst, i, j);
                failed[i][j] = true;
                failed[j][i] = true;
                ++num_failed_links;
            }
        } else if (num_failed_links > 0) {
            for (size_t i = 0; i < n; ++i) {
                for (size_t j = i + 1; j < n; ++j) {
                    if (failed[i][j] && tester.randomness.coin_flip(0.1)) {
                        recover(inst, i, j, base_loss);
                        failed[i][j] = false;
                        failed[j][i] = false;
                        --num_failed_links;
                    }
                }
            }
        }
    }
}

cot::task<> disruptive_isolate_routine(pt_paxos_instance& inst,
                                       cot::duration split_time = 10s,
                                       cot::duration heal_time = 10s) {
    co_await cot::after(split_time);

    size_t victim = inst.tester.nreplicas;
    std::vector<size_t> followers;
    followers.reserve(inst.tester.nreplicas);
    for (size_t s = 0; s < inst.tester.nreplicas; ++s) {
        if (inst.replicas[s]->leader_index_ != s)
            followers.push_back(s);
    }

    if (!followers.empty()) {
        victim = followers[inst.tester.randomness.uniform<size_t>(0, followers.size() - 1)];
        for (size_t s = 0; s < inst.tester.nreplicas; ++s) {
            if (s != victim)
                inst.replicas[s]->to_replicas_[victim]->set_loss(1.0);
        }
    }

    co_await cot::after(heal_time);

    if (victim != inst.tester.nreplicas) {
        for (size_t s = 0; s < inst.tester.nreplicas; ++s) {
            if (s != victim)
                inst.replicas[s]->to_replicas_[victim]->set_loss(inst.tester.loss);
        }
    }
}

cot::task<> clear_after(cot::duration d) {
    co_await cot::after(d);
    cot::clear();
}

bool try_one_seed(testinfo& tester, unsigned long seed) {
    cot::reset();   // clear old events and coroutines
    tester.randomness.seed(seed);
    tester.excluded_replicas.clear();

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
        case failure_mode::unstable_leader_mixed: {
            std::vector<size_t> followers;
            followers.reserve(tester.nreplicas - 1);
            for (size_t s = 0; s != tester.nreplicas; ++s) {
                if (s != tester.initial_leader) {
                    followers.push_back(s);
                }
            }

            std::shuffle(followers.begin(), followers.end(), tester.randomness.engine());

            size_t flapping_count = followers.size() / 2;
            std::vector<size_t> flapping_followers(
                followers.begin(), followers.begin() + flapping_count
            );
            std::vector<size_t> partitioned_followers(
                followers.begin() + flapping_count, followers.end()
            );

            tester.excluded_replicas = partitioned_followers;

            std::vector<size_t> connected_group{tester.initial_leader};
            connected_group.insert(connected_group.end(),
                                   flapping_followers.begin(),
                                   flapping_followers.end());

            tasks.push_back(partition_groups_forever(
                inst, connected_group, partitioned_followers
            ));
            tasks.push_back(up_down_randomly(
                inst, static_cast<int>(tester.initial_leader), 3s
            ));
            for (size_t s : flapping_followers) {
                tasks.push_back(up_down_randomly(inst, static_cast<int>(s), 3s));
            }
            break;
        }
        case failure_mode::random_failure_schedule:
            tasks.push_back(random_failure_schedule_task(inst));
            break;
        case failure_mode::william_link_schedule:
            tasks.push_back(random_link_schedule(inst, tester));
            break;
        case failure_mode::disruptive_isolate:
            tasks.push_back(disruptive_isolate_routine(inst));
            break;
        case failure_mode::split_brain:
            apply_split_brain(inst);
            break;
        case failure_mode::minority_partition_failure:
            tasks.push_back(minority_partition(inst, tester.initial_leader, 20s));
            break;
        case failure_mode::vihaan_split_brain_failure:
            tasks.push_back(failure_split_brain(inst, 10s, 20s));
            break;
        case failure_mode::cascading_star_partition:
            tasks.push_back(cascading_star_partition_scenario(inst));
            break;
        case failure_mode::split_brain_isolate_heal:
            tasks.push_back(split_brain_isolate_heal_schedule(inst, tester));
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
    if (tester.mode == failure_mode::cascading_star_partition
        || tester.mode == failure_mode::unstable_leader_mixed
        || tester.mode == failure_mode::random_failure_schedule) {
        for (size_t s = 0; s != tester.nreplicas; ++s) {
            if (std::find(tester.excluded_replicas.begin(),
                          tester.excluded_replicas.end(),
                          s) == tester.excluded_replicas.end()) {
                reference = s;
                break;
            }
        }
    }
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
        if (tester.mode == failure_mode::cascading_star_partition
            && std::find(tester.excluded_replicas.begin(),
                         tester.excluded_replicas.end(),
                         s) != tester.excluded_replicas.end())
            continue;
        if (tester.mode == failure_mode::unstable_leader_mixed
            && std::find(tester.excluded_replicas.begin(),
                         tester.excluded_replicas.end(),
                         s) != tester.excluded_replicas.end())
            continue;
        if (tester.mode == failure_mode::random_failure_schedule
            && std::find(tester.excluded_replicas.begin(),
                         tester.excluded_replicas.end(),
                         s) != tester.excluded_replicas.end())
            continue;
        auto problem = db.diff(
            inst.replicas[s]->db_,
            tester.mode == failure_mode::cascading_star_partition ? 15 : 5
        );
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
            } else if (strcmp(optarg, "unstable_leader_mixed") == 0) {
                tester.mode = failure_mode::unstable_leader_mixed;
            } else if (strcmp(optarg, "random_failure_schedule") == 0) {
                tester.mode = failure_mode::random_failure_schedule;
            } else if (strcmp(optarg, "william_link_schedule") == 0
                       || strcmp(optarg, "random_link_schedule") == 0) {
                tester.mode = failure_mode::william_link_schedule;
            } else if (strcmp(optarg, "disruptive_isolate") == 0) {
                tester.mode = failure_mode::disruptive_isolate;
            } else if (strcmp(optarg, "split_brain") == 0) {
                tester.mode = failure_mode::split_brain;
            } else if (strcmp(optarg, "minority_partition") == 0) {
                tester.mode = failure_mode::minority_partition_failure;
            } else if (strcmp(optarg, "vihaan_split_brain") == 0) {
                tester.mode = failure_mode::vihaan_split_brain_failure;
            } else if (strcmp(optarg, "cascading_star_partition") == 0) {
                tester.mode = failure_mode::cascading_star_partition;
            } else if (strcmp(optarg, "split_brain_isolate_heal") == 0) {
                tester.mode = failure_mode::split_brain_isolate_heal;
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
