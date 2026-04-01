#pragma once
#include <format>
#include <variant>
#include <deque>
#include "pancy_msgs.hh"


struct base_message {
    unsigned long long round;
};

struct propose_msg : base_message {
    unsigned long long committed_slot = 0; // decide shortcut
    unsigned leader_id = 0;

    unsigned long long prev_slot;
    unsigned long long prev_round;

    unsigned long long batch_start = 0;
    std::deque<pancy::request> entries; // empty = raft heartbeat keepalive
};

struct probe_msg : base_message {
    unsigned leader_id = 0;
};

struct prepare_msg : base_message {
    unsigned long long accepted_round = 0;
    std::deque<pancy::request> accepted_values;
};

struct ack_msg : base_message {
    bool success = false;
    unsigned long long highest_accepted = 0;
};


using paxos_message = std::variant<propose_msg, probe_msg, prepare_msg, ack_msg>;


// thanks claude for these!
namespace std {

template <typename CharT>
struct formatter<propose_msg, CharT> {
    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
    template <typename FormatContext>
    auto format(const propose_msg& m, FormatContext& ctx) const {
        return std::format_to(ctx.out(),
            "PROPOSE(round={}, leader={}, committed={}, prev=[{},{}], batch_start={}, entries={})",
            m.round, m.leader_id, m.committed_slot,
            m.prev_slot, m.prev_round,
            m.batch_start, m.entries.size());
    }
};

template <typename CharT>
struct formatter<ack_msg, CharT> {
    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
    template <typename FormatContext>
    auto format(const ack_msg& m, FormatContext& ctx) const {
        return std::format_to(ctx.out(),
            "ACK(round={}, success={}, highest_accepted={})",
            m.round, m.success, m.highest_accepted);
    }
};

template <typename CharT>
struct formatter<probe_msg, CharT> {
    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
    template <typename FormatContext>
    auto format(const probe_msg& m, FormatContext& ctx) const {
        return std::format_to(ctx.out(),
            "PROBE(round={}, leader={})",
            m.round, m.leader_id);
    }
};

template <typename CharT>
struct formatter<prepare_msg, CharT> {
    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
    template <typename FormatContext>
    auto format(const prepare_msg& m, FormatContext& ctx) const {
        return std::format_to(ctx.out(),
            "PREPARE(round={}, accepted_round={}, accepted_values={})",
            m.round, m.accepted_round, m.accepted_values.size());
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