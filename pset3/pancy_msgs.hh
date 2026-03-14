#pragma once
#include <cstdint>
#include <format>
#include <string>
#include <type_traits>
#include <variant>

// pancy_msgs.hh
//
//    This file defines message types for messages between clients and a Pancy
//    service. (Pancy = Paxos + Nancy is fun to say.)
//
//    Requests from clients use a `*_request` type. Responses typically use
//    the corresponding `*_response` type, but a server can tell a client to
//    contact another server by sending a `redirection_response`.
//
//    The `serial` number distinguishes messages. A Pancy service echoes the
//    request’s `serial` in its response. Our client models use the lower bits
//    of the `serial` as a unique client ID.
//
//    Responses have an `errcode` that indicates whether there was an error.

namespace pancy {

// Type used for data version numbers; actual data have positive versions
using version_type = int64_t;
// Represents absent keys
constexpr version_type nonexistent_version = 0;
// Used in request `version_match` to match any version
constexpr version_type any_version = -1;

// Error codes
enum class errc {
    ok = 0,
    not_found = -1,
    no_match = -2,
    redirect = -3
};

// Message headers
struct message_base {
    uint64_t serial;
};
struct request_base : public message_base { };
struct response_base : public message_base {
    errc errcode;
};


// Response types
struct get_response;
struct put_response;
struct cas_response;
struct remove_response;


// Special response: client should redirect to new leader
struct redirection_response : public response_base {
    size_t redirection;   // Replica index of leader
};


// Get value for key
struct get_request : public request_base {
    using response_type = get_response;
    std::string key;
};
struct get_response : public response_base {
    std::string value;
    version_type version;
};


// Put value at key
struct put_request : public request_base {
    using response_type = put_response;
    std::string key;
    std::string value;
    version_type version_match = any_version;
};
struct put_response : public response_base {
    std::string actual;     // Actual value (only set if errc::no_match)
    version_type previous_version;  // Value version before modification
    version_type version;           // Value version after modification
};


// Compare-exchange operation
struct cas_request : public request_base {
    using response_type = cas_response;
    std::string key;        // Key to check
    std::string expected;   // Expected value (empty string matches absent)
    std::string desired;    // Desired new value
    version_type version_match = any_version;
};
struct cas_response : public response_base {
    std::string actual;     // Actual value (only set if errc::no_match)
    version_type previous_version;
    version_type version;
};


// Remove operation
struct remove_request : public request_base {
    using response_type = remove_response;
    std::string key;
    version_type version_match = any_version;
};
struct remove_response : public response_base {
    std::string actual;     // Actual value (only set if errc::no_match)
    version_type previous_version;
    version_type version;
};


// Variants (types for channels)
using request = std::variant<
    get_request,
    put_request,
    cas_request,
    remove_request
>;

using response = std::variant<
    redirection_response,
    get_response,
    put_response,
    cas_response,
    remove_response
>;

using message = std::variant<
    get_request,
    put_request,
    cas_request,
    remove_request,
    redirection_response,
    get_response,
    put_response,
    cas_response,
    remove_response
>;


// Concepts
template <typename T>
concept message_type = std::is_base_of_v<message_base, T>;
template <typename T>
concept request_type = std::is_base_of_v<request_base, T>;
template <typename T>
concept response_type = std::is_base_of_v<response_base, T>;


// Message introspection
inline constexpr const char* name(const get_request&) { return "GET"; }
inline constexpr const char* name(const get_response&) { return "GET_A"; }
inline constexpr const char* name(const put_request&) { return "PUT"; }
inline constexpr const char* name(const put_response&) { return "PUT_A"; }
inline constexpr const char* name(const cas_request&) { return "CAS"; }
inline constexpr const char* name(const cas_response&) { return "CAS_A"; }
inline constexpr const char* name(const remove_request&) { return "REMOVE"; }
inline constexpr const char* name(const remove_response&) { return "REMOVE_A"; }
inline constexpr const char* name(const redirection_response&) { return "REDIRECTION"; }

// message_serial(m) - return `serial` of message or variant
inline constexpr uint64_t message_serial(const message_base& m) noexcept {
    return m.serial;
}
inline constexpr uint64_t message_serial(const message& m) noexcept {
    return std::visit([](auto&& reqt) -> uint64_t {
        return reqt.serial;
    }, m);
}
inline constexpr uint64_t message_serial(const request& m) noexcept {
    return std::visit([](auto&& reqt) -> uint64_t {
        return reqt.serial;
    }, m);
}
inline constexpr uint64_t message_serial(const response& m) noexcept {
    return std::visit([](auto&& respt) -> uint64_t {
        return respt.serial;
    }, m);
}

// response_errcode(resp) - return `errcode` of response or variant
inline constexpr errc response_errcode(const response_base& resp) noexcept {
    return resp.errcode;
}
inline constexpr errc response_errcode(const response& resp) noexcept {
    return std::visit([](auto&& respt) -> errc {
        return respt.errcode;
    }, resp);
}

// response_header(req) - return a `response_base` suitable for responding to
// the given `req`
inline constexpr response_base response_header(const request_base& req,
                                               errc errcode = errc()) noexcept {
    return {{req.serial}, errcode};
}
inline constexpr response_base response_header(const request& req,
                                               errc errcode = errc()) noexcept {
    return {{message_serial(req)}, errcode};
}

struct version_match_formatter {
    version_type version_match;
};

struct modification_formatter {
    errc errcode;
    std::string actual;
    version_type previous_version;
    version_type version;
};

}



// - `std::format` and `std::print` support for requests and responses

namespace std {

template <typename CharT>
struct formatter<pancy::errc, CharT> : formatter<const char*, CharT> {
    using parent = formatter<const char*, CharT>;
    template <typename FormatContext>
    auto format(pancy::errc errcode, FormatContext& ctx) const {
        if (errcode == pancy::errc::ok) {
            return parent::format("✓", ctx);
        } else if (errcode == pancy::errc::not_found) {
            return parent::format("ENOTFOUND", ctx);
        } else if (errcode == pancy::errc::no_match) {
            return parent::format("ENOMATCH", ctx);
        } else if (errcode == pancy::errc::redirect) {
            return parent::format("EREDIRECT", ctx);
        }
        return parent::format("EUNKNOWN", ctx);
    }
};

template <typename CharT>
struct formatter<pancy::version_match_formatter, CharT> {
    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
    template <typename FormatContext>
    auto format(pancy::version_match_formatter vm, FormatContext& ctx) const {
        if (vm.version_match < 0) {
            return ctx.out();
        }
        return std::format_to(ctx.out(), ", VM{}", vm.version_match);
    }
};

template <typename CharT>
struct formatter<pancy::modification_formatter, CharT> {
    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
    template <typename FormatContext>
    auto format(pancy::modification_formatter mod, FormatContext& ctx) const {
        if (mod.errcode == pancy::errc::no_match) {
            return std::format_to(ctx.out(), "{}, \"{}\", V{}", mod.errcode, mod.actual, mod.previous_version);
        } else if (mod.previous_version == mod.version) {
            return std::format_to(ctx.out(), "{}, V{}", mod.errcode, mod.previous_version);
        }
        return std::format_to(ctx.out(), "{}, V{}→{}", mod.errcode, mod.previous_version, mod.version);
    }
};

template <pancy::request_type M, typename CharT>
struct formatter<M, CharT> {
    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
    template <typename FormatContext>
    auto format(const M& m, FormatContext& ctx) const {
        if constexpr (is_same_v<M, pancy::get_request>) {
            return std::format_to(ctx.out(), "GET(#{}, \"{}\")", m.serial, m.key);
        } else if constexpr (is_same_v<M, pancy::put_request>) {
            return std::format_to(ctx.out(), "PUT(#{}, \"{}\", \"{}\"{})", m.serial, m.key, m.value, pancy::version_match_formatter{m.version_match});
        } else if constexpr (is_same_v<M, pancy::cas_request>) {
            return std::format_to(ctx.out(), "CAS(#{}, \"{}\", \"{}\", \"{}\"{})", m.serial, m.key, m.expected, m.desired, pancy::version_match_formatter{m.version_match});
        } else if constexpr (is_same_v<M, pancy::remove_request>) {
            return std::format_to(ctx.out(), "REMOVE(#{}, \"{}\"{})", m.serial, m.key, pancy::version_match_formatter{m.version_match});
        } else {
            static_assert(false && "unknown pancy::request_type");
        }
    }
};

template <pancy::response_type M, typename CharT>
struct formatter<M, CharT> {
    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
    template <typename FormatContext>
    auto format(const M& m, FormatContext& ctx) const {
        if constexpr (is_same_v<M, pancy::get_response>) {
            return std::format_to(ctx.out(), "GET_A(#{}, {}, \"{}\", V{})", m.serial, m.errcode, m.value, m.version);
        } else if constexpr (is_same_v<M, pancy::put_response>) {
            return std::format_to(ctx.out(), "PUT_A(#{}, {})", m.serial, pancy::modification_formatter{m.errcode, m.actual, m.previous_version, m.version});
        } else if constexpr (is_same_v<M, pancy::cas_response>) {
            return std::format_to(ctx.out(), "CAS_A(#{}, {})", m.serial, pancy::modification_formatter{m.errcode, m.actual, m.previous_version, m.version});
        } else if constexpr (is_same_v<M, pancy::remove_response>) {
            return std::format_to(ctx.out(), "REMOVE_A(#{}, {})", m.serial, pancy::modification_formatter{m.errcode, m.actual, m.previous_version, m.version});
        } else if constexpr (is_same_v<M, pancy::redirection_response>) {
            return std::format_to(ctx.out(), "REDIRECTION(#{}, ↪{})", m.serial, m.redirection);
        } else {
            static_assert(false && "unknown pancy::response_type");
        }
    }
};

template <typename CharT>
struct formatter<pancy::request, CharT> {
    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
    template <typename FormatContext>
    auto format(const pancy::request& m, FormatContext& ctx) const {
        return std::visit([&](auto&& reqt) -> FormatContext::iterator {
            return std::format_to(ctx.out(), "{}", reqt);
        }, m);
    }
};

template <typename CharT>
struct formatter<pancy::response, CharT> {
    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
    template <typename FormatContext>
    auto format(const pancy::response& m, FormatContext& ctx) const {
        return std::visit([&](auto&& reqt) -> FormatContext::iterator {
            return std::format_to(ctx.out(), "{}", reqt);
        }, m);
    }
};

template <typename CharT>
struct formatter<pancy::message, CharT> {
    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
    template <typename FormatContext>
    auto format(const pancy::message& m, FormatContext& ctx) const {
        return std::visit([&](auto&& reqt) -> FormatContext::iterator {
            return std::format_to(ctx.out(), "{}", reqt);
        }, m);
    }
};

}
