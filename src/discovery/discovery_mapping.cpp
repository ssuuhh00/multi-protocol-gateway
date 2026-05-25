#include "discovery/discovery_mapping.hpp"

#include <fstream>
#include <stdexcept>
#include <string>

#include "json/json.hpp"
#include "ros2_msg_type_registry.hpp"

namespace {

ProtocolType parseProtocol(const std::string& s) {
    if (s == "DDS") return ProtocolType::DDS;
    if (s == "SOMEIP") return ProtocolType::SOMEIP;
    if (s == "ZENOH") return ProtocolType::ZENOH;
    throw std::runtime_error("Unknown protocol: " + s + " (expected DDS|SOMEIP|ZENOH)");
}

Qos parseQos(const std::string& s) {
    if (s == "RELIABLE") return Qos::RELIABLE;
    if (s == "BEST_EFFORT") return Qos::BEST_EFFORT;
    throw std::runtime_error("Unknown qos: " + s + " (expected RELIABLE|BEST_EFFORT)");
}

MessageType parseMessageType(const std::string& s) {
    MessageType t = DdsTypeRegistry::stringToEnum(s);
    if (t == MessageType::NONE) {
        throw std::runtime_error("Unknown message_type: " + s);
    }
    return t;
}

void requireField(const nlohmann::json& obj, const char* key, const std::string& mapping_id) {
    if (!obj.contains(key)) {
        throw std::runtime_error("mapping[" + mapping_id + "] missing required field: " + key);
    }
}

}  // namespace

std::vector<DiscoveryMapping> loadDiscoveryMappingsFromJson(const std::string& path) {
    std::ifstream f(path);
    if (!f.is_open()) {
        throw std::runtime_error("Cannot open mapping config: " + path);
    }

    nlohmann::json root;
    try {
        f >> root;
    } catch (const nlohmann::json::parse_error& e) {
        throw std::runtime_error(std::string("JSON parse error in ") + path + ": " + e.what());
    }

    if (!root.contains("mappings") || !root["mappings"].is_array()) {
        throw std::runtime_error("Config must contain top-level array 'mappings'");
    }

    std::vector<DiscoveryMapping> out;
    out.reserve(root["mappings"].size());

    for (size_t i = 0; i < root["mappings"].size(); ++i) {
        const auto& m = root["mappings"][i];
        const std::string id = std::to_string(i);

        requireField(m, "source", id);
        requireField(m, "sink", id);
        requireField(m, "message_type", id);
        requireField(m, "qos", id);

        Command cmd;
        cmd.source = parseProtocol(m["source"].get<std::string>());
        cmd.sink = parseProtocol(m["sink"].get<std::string>());
        cmd.message_type = parseMessageType(m["message_type"].get<std::string>());
        cmd.qos = parseQos(m["qos"].get<std::string>());
        cmd.bench_mode = m.value("bench_mode", false);

        const bool uses_dds = (cmd.source == ProtocolType::DDS || cmd.sink == ProtocolType::DDS);
        const bool uses_zenoh = (cmd.source == ProtocolType::ZENOH || cmd.sink == ProtocolType::ZENOH);
        const bool uses_someip = (cmd.source == ProtocolType::SOMEIP || cmd.sink == ProtocolType::SOMEIP);

        if (uses_dds) {
            requireField(m, "topic", id);
            cmd.topic = m["topic"].get<std::string>();
        }
        if (uses_zenoh) {
            requireField(m, "zenoh_key", id);
            cmd.zenoh_key = m["zenoh_key"].get<std::string>();
        }
        if (uses_someip) {
            requireField(m, "service_id", id);
            requireField(m, "instance_id", id);
            requireField(m, "eventgroup_id", id);
            requireField(m, "event_id", id);
            cmd.service_id = m["service_id"].get<uint16_t>();
            cmd.instance_id = m["instance_id"].get<uint16_t>();
            cmd.eventgroup_id = m["eventgroup_id"].get<uint16_t>();
            cmd.event_id = m["event_id"].get<uint16_t>();
        }

        DiscoveryMapping dm;
        dm.cmd_template = cmd;
        out.push_back(std::move(dm));
    }

    return out;
}
