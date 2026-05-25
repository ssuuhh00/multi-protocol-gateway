#ifndef COMMON_ZENOH_LIVELINESS_HPP
#define COMMON_ZENOH_LIVELINESS_HPP

#include <string>
#include <string_view>

namespace mpg::zenoh_liveliness {

// Reserved admin-namespace prefix. Zenoh keyexpr chunks starting with '@' are
// excluded from '**' wildcard matching, so liveliness traffic stays isolated
// from data flows on the same network.
inline constexpr const char* kPrefix = "@mpg";

// Role placed in chunk[1].
inline constexpr const char* kRolePub = "pub";
inline constexpr const char* kRoleSub = "sub";

// Build the keyexpr a Zenoh endpoint should declare a LivelinessToken on so
// that peers can detect its presence.
//   role      : "pub" or "sub"
//   zid_hex   : session.get_zid().to_string() of the declaring endpoint
//   user_key  : the data keyexpr the endpoint is publishing/subscribing on
//
//   result    : "@mpg/<role>/<zid_hex>/<user_key>"
inline std::string make_token_key(std::string_view role, std::string_view zid_hex, std::string_view user_key) {
    std::string out;
    out.reserve(/*prefix*/ 5 + role.size() + 1 + zid_hex.size() + 1 + user_key.size());
    out.append(kPrefix);
    out.push_back('/');
    out.append(role.data(), role.size());
    out.push_back('/');
    out.append(zid_hex.data(), zid_hex.size());
    out.push_back('/');
    out.append(user_key.data(), user_key.size());
    return out;
}

// Build the keyexpr the gateway's DiscoveryManager subscribes to in order to
// detect peers (any zid) acting in the given role on user_key.
//   result : "@mpg/<role>/*/<user_key>"
inline std::string make_subscribe_key(std::string_view role, std::string_view user_key) {
    std::string out;
    out.reserve(/*prefix*/ 5 + role.size() + 3 + user_key.size());
    out.append(kPrefix);
    out.push_back('/');
    out.append(role.data(), role.size());
    out.append("/*/", 3);
    out.append(user_key.data(), user_key.size());
    return out;
}

// Extract the zid chunk (chunk[2]) from a matched liveliness keyexpr.
// Returns empty string_view if the keyexpr doesn't have the expected shape.
inline std::string_view parse_zid(std::string_view matched_key) {
    // Layout: @mpg / <role> / <zid> / <user_key...>
    // Find positions of slashes #1 and #2.
    size_t s1 = matched_key.find('/');
    if (s1 == std::string_view::npos) return {};
    size_t s2 = matched_key.find('/', s1 + 1);
    if (s2 == std::string_view::npos) return {};
    size_t s3 = matched_key.find('/', s2 + 1);
    if (s3 == std::string_view::npos) return {};
    return matched_key.substr(s2 + 1, s3 - (s2 + 1));
}

}  // namespace mpg::zenoh_liveliness

#endif  // COMMON_ZENOH_LIVELINESS_HPP
