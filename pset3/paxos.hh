#pragma once
#include <variant>
#include <deque>
#include "pancy_msgs.hh"


struct base_message {
    unsigned long long round;
};

struct prepare_msg : base_message {
    unsigned long long committed_slot = 0; // decide shortcut
    unsigned leader_id = 0;

    unsigned long long prev_slot;
    unsigned long long prev_round;

    unsigned long long batch_start = 0;
    std::deque<pancy::request> entries; // empty = raft heartbeat keepalive
};

struct ack_msg : base_message {
    bool success = false;
    unsigned long long highest_accepted = 0;
};


using paxos_message = std::variant<prepare_msg, ack_msg>;