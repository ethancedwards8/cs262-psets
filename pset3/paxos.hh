#pragma once
#include <cstddef>
#include <deque>
#include <format>
#include <variant>
#include "pancy_msgs.hh"


// Compatibility layer for the pset3 harness.
//
// The older Paxos-like test driver still expects these round-based message
// types, while newer Raft-oriented code uses the request-vote / append-entries
// API below. Keep both sets available so the header can serve either shape.
struct base_message {
    unsigned long long round = 0;
};

struct probe_msg : base_message {
    // round number only (inherited)
};

struct prepare_msg : base_message {
    unsigned long long accepted_round = 0;
    unsigned long long applied_up_to = 0;
    std::deque<pancy::request> accepted_values;
};

struct propose_msg : base_message {
    unsigned long long committed_slot = 0; // decide shortcut
    unsigned long long batch_start = 0;
    std::deque<pancy::request> entries; // empty = raft heartbeat keepalive
};

struct ack_msg : base_message {
    bool success = false;
    unsigned long long highest_accepted = 0;
    unsigned long long applied_up_to = 0;
};


// Raft-oriented messages.
struct log_entry {
    unsigned long long term = 0;
    pancy::request command;
};

struct request_vote_request {
    unsigned long long term = 0;
    std::size_t candidate_id = 0;
    unsigned long long last_log_index = 0;
    unsigned long long last_log_term = 0;
};

struct request_vote_response {
    unsigned long long term = 0;
    bool vote_granted = false;
};

struct append_entries_request {
    unsigned long long term = 0;
    std::size_t leader_id = 0;
    unsigned long long prev_log_index = 0;
    unsigned long long prev_log_term = 0;
    std::deque<log_entry> entries;
    unsigned long long leader_commit = 0;
};

struct append_entries_response {
    unsigned long long term = 0;
    bool success = false;
    unsigned long long conflict_term = 0;
    unsigned long long conflict_index = 0;
    unsigned long long match_index = 0;
};

using request_vote_msg = request_vote_request;
using request_vote_response_msg = request_vote_response;
using append_entries_msg = append_entries_request;
using append_entries_response_msg = append_entries_response;


using raft_message = std::variant<
    request_vote_request,
    request_vote_response,
    append_entries_request,
    append_entries_response
>;

// Keep the public alias name that the harness already uses.
using paxos_message = std::variant<
    propose_msg,
    probe_msg,
    prepare_msg,
    ack_msg,
    request_vote_request,
    request_vote_response,
    append_entries_request,
    append_entries_response
>;


// thanks claude for these!
namespace std {

template <typename CharT>
struct formatter<log_entry, CharT> {
    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
    template <typename FormatContext>
    auto format(const log_entry& m, FormatContext& ctx) const {
        return std::format_to(ctx.out(), "ENTRY(term={}, command={})", m.term, m.command);
    }
};

template <typename CharT>
struct formatter<request_vote_request, CharT> {
    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
    template <typename FormatContext>
    auto format(const request_vote_request& m, FormatContext& ctx) const {
        return std::format_to(ctx.out(),
            "REQUEST_VOTE(term={}, candidate_id={}, last_log_index={}, last_log_term={})",
            m.term, m.candidate_id, m.last_log_index, m.last_log_term);
    }
};

template <typename CharT>
struct formatter<request_vote_response, CharT> {
    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
    template <typename FormatContext>
    auto format(const request_vote_response& m, FormatContext& ctx) const {
        return std::format_to(ctx.out(),
            "REQUEST_VOTE_A(term={}, vote_granted={})",
            m.term, m.vote_granted);
    }
};

template <typename CharT>
struct formatter<append_entries_request, CharT> {
    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
    template <typename FormatContext>
    auto format(const append_entries_request& m, FormatContext& ctx) const {
        return std::format_to(ctx.out(),
            "APPEND_ENTRIES(term={}, leader_id={}, prev_log_index={}, prev_log_term={}, entries={}, leader_commit={})",
            m.term, m.leader_id, m.prev_log_index, m.prev_log_term,
            m.entries.size(), m.leader_commit);
    }
};

template <typename CharT>
struct formatter<append_entries_response, CharT> {
    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
    template <typename FormatContext>
    auto format(const append_entries_response& m, FormatContext& ctx) const {
        return std::format_to(ctx.out(),
            "APPEND_ENTRIES_A(term={}, success={}, conflict_term={}, conflict_index={}, match_index={})",
            m.term, m.success, m.conflict_term, m.conflict_index, m.match_index);
    }
};

template <typename CharT>
struct formatter<propose_msg, CharT> {
    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
    template <typename FormatContext>
    auto format(const propose_msg& m, FormatContext& ctx) const {
        return std::format_to(ctx.out(),
            "PROPOSE(round={}, committed={}, batch_start={}, entries={})",
            m.round, m.committed_slot,
            m.batch_start, m.entries.size());
    }
};

template <typename CharT>
struct formatter<ack_msg, CharT> {
    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
    template <typename FormatContext>
    auto format(const ack_msg& m, FormatContext& ctx) const {
        return std::format_to(ctx.out(),
            "ACK(round={}, success={}, highest_accepted={}, applied_up_to={})",
            m.round, m.success, m.highest_accepted, m.applied_up_to);
    }
};

template <typename CharT>
struct formatter<probe_msg, CharT> {
    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
    template <typename FormatContext>
    auto format(const probe_msg& m, FormatContext& ctx) const {
        return std::format_to(ctx.out(),
            "PROBE(round={})",
            m.round);
    }
};

template <typename CharT>
struct formatter<prepare_msg, CharT> {
    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
    template <typename FormatContext>
    auto format(const prepare_msg& m, FormatContext& ctx) const {
        return std::format_to(ctx.out(),
            "PREPARE(round={}, accepted_round={}, applied_up_to={}, accepted_values={})",
            m.round, m.accepted_round, m.applied_up_to, m.accepted_values.size());
    }
};

template <typename CharT>
struct formatter<raft_message, CharT> {
    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
    template <typename FormatContext>
    auto format(const raft_message& m, FormatContext& ctx) const {
        return std::visit([&](auto&& msg) -> FormatContext::iterator {
            return std::format_to(ctx.out(), "{}", msg);
        }, m);
    }
};

template <typename CharT>
struct formatter<paxos_message, CharT> {
    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
    template <typename FormatContext>
    auto format(const paxos_message& m, FormatContext& ctx) const {
        return std::visit([&](auto&& msg) -> FormatContext::iterator {
            return std::format_to(ctx.out(), "{}", msg);
        }, m);
    }
};

}
