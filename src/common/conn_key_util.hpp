#ifndef COMMON_CONN_KEY_UTIL_HPP
#define COMMON_CONN_KEY_UTIL_HPP

#include <spdlog/spdlog.h>

#include <iomanip>
#include <optional>
#include <sstream>
#include <string>

#include "command.hpp"

// Encode (source, sink) as a unique routing key. Returns "" if a required
// address field is missing or both sides are NONE.
//
// Format     : "<source>>><sink>"   (the literal separator is '>>')
// Prefixes   : D = DDS, S = SOME/IP, Z = Zenoh
// Examples   : "D:rt/my_topic>>Z:zenoh/key"
//              "S:1234:5678:1:8001>>D:rt/my_topic"
//              "D:rt/my_topic>>NONE"           (single-endpoint source)
//              "NONE>>Z:zenoh/key"             (single-endpoint sink)
inline std::string make_conn_key(const Command& cmd) {
    std::stringstream source_ss;
    std::stringstream sink_ss;
    source_ss << std::hex << std::uppercase;
    sink_ss << std::hex << std::uppercase;

    if (cmd.source == ProtocolType::DDS) {
        if (cmd.topic.empty()) return "";
        source_ss << "D:" << cmd.topic;
    } else if (cmd.source == ProtocolType::SOMEIP) {
        if (!cmd.service_id.has_value() || !cmd.instance_id.has_value() || !cmd.eventgroup_id.has_value() ||
            !cmd.event_id.has_value())
            return "";
        source_ss << "S:" << cmd.service_id.value() << ":" << cmd.instance_id.value() << ":"
                  << cmd.eventgroup_id.value() << ":" << cmd.event_id.value();
    } else if (cmd.source == ProtocolType::ZENOH) {
        if (cmd.zenoh_key.empty()) return "";
        source_ss << "Z:" << cmd.zenoh_key;
    } else {
        source_ss << "NONE";
    }

    if (cmd.sink == ProtocolType::DDS) {
        if (cmd.topic.empty()) return "";
        sink_ss << "D:" << cmd.topic;
    } else if (cmd.sink == ProtocolType::SOMEIP) {
        if (!cmd.service_id.has_value() || !cmd.instance_id.has_value() || !cmd.eventgroup_id.has_value() ||
            !cmd.event_id.has_value())
            return "";
        sink_ss << "S:" << cmd.service_id.value() << ":" << cmd.instance_id.value() << ":" << cmd.eventgroup_id.value()
                << ":" << cmd.event_id.value();
    } else if (cmd.sink == ProtocolType::ZENOH) {
        if (cmd.zenoh_key.empty()) return "";
        sink_ss << "Z:" << cmd.zenoh_key;
    } else {
        sink_ss << "NONE";
    }

    std::string source_str = source_ss.str();
    std::string sink_str = sink_ss.str();
    if (source_str == "NONE" && sink_str == "NONE") return "";

    return source_str + ">>" + sink_str;
}

#endif  // COMMON_CONN_KEY_UTIL_HPP
