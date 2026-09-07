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

// --- fine-grained QoS ------------------------------------------------------
// Every parse below is strict: an unknown value throws rather than silently
// falling back to a default. Accepting a policy we then ignore is the failure
// mode the reviewers are pointing at, so it must not be reachable from config.

ReliabilityKind parseReliability(const std::string& s, const std::string& where) {
    if (s == "RELIABLE") return ReliabilityKind::RELIABLE;
    if (s == "BEST_EFFORT") return ReliabilityKind::BEST_EFFORT;
    throw std::runtime_error(where + ": unknown reliability '" + s + "' (expected RELIABLE|BEST_EFFORT)");
}

HistoryKind parseHistoryKind(const std::string& s, const std::string& where) {
    if (s == "KEEP_LAST") return HistoryKind::KEEP_LAST;
    if (s == "KEEP_ALL") return HistoryKind::KEEP_ALL;
    throw std::runtime_error(where + ": unknown history kind '" + s + "' (expected KEEP_LAST|KEEP_ALL)");
}

DurabilityKind parseDurability(const std::string& s, const std::string& where) {
    if (s == "VOLATILE") return DurabilityKind::VOLATILE;
    if (s == "TRANSIENT_LOCAL") return DurabilityKind::TRANSIENT_LOCAL;
    throw std::runtime_error(where + ": unknown durability '" + s + "' (expected VOLATILE|TRANSIENT_LOCAL)");
}

/**
 * Parse the declared profile. Absent keys stay unset -- that is the whole point
 * of the optionals, so do not write defaults here.
 *
 * One profile for the mapping, not one per leg. A QoS declaration is a statement
 * about the data flowing across the bridge, and the bridge is one path; what
 * each leg is then built with is fixed per (policy, direction) by the spec
 * sheet (common/qos_spec.hpp) rather than restated by the operator twice. It
 * also matches the `qos` field this sits beside, which the submitted gateway
 * already shared between both legs.
 */
QosProfile parseQosProfile(const nlohmann::json& j, const std::string& where) {
    QosProfile p;
    if (!j.is_object()) {
        throw std::runtime_error(where + ": qos_policies must be an object");
    }

    for (auto it = j.begin(); it != j.end(); ++it) {
        const std::string& key = it.key();
        if (key == "reliability") {
            p.reliability = parseReliability(it->get<std::string>(), where);
        } else if (key == "history") {
            HistorySpec h;
            if (it->is_string()) {
                h.kind = parseHistoryKind(it->get<std::string>(), where);  // "KEEP_ALL" shorthand
            } else {
                requireField(*it, "kind", where);
                h.kind = parseHistoryKind((*it)["kind"].get<std::string>(), where);
                h.depth = it->value("depth", 1);
            }
            if (h.kind == HistoryKind::KEEP_LAST && h.depth < 1) {
                throw std::runtime_error(where + ": history depth must be >= 1 for KEEP_LAST");
            }
            p.history = h;
        } else if (key == "deadline_ms") {
            const auto ms = it->get<int64_t>();
            if (ms <= 0) {
                throw std::runtime_error(where + ": deadline_ms must be > 0");
            }
            p.deadline_ms = static_cast<uint32_t>(ms);
        } else if (key == "liveliness") {
            // Declared or not, and nothing else. The gateway builds liveliness
            // in one shape on every leg (AUTOMATIC, no lease -- qos_profile.hpp),
            // so a kind or a lease here would be a value read and not
            // enforced. Rejecting it is the point: "liveliness": true is the
            // whole declaration.
            if (!it->is_boolean()) {
                throw std::runtime_error(where +
                                         ": liveliness takes no parameters -- it is built as AUTOMATIC with no "
                                         "lease on every leg. Write \"liveliness\": true");
            }
            if (it->get<bool>()) {
                p.liveliness = LivelinessSpec{};
            }
        } else if (key == "durability") {
            p.durability = parseDurability(it->get<std::string>(), where);
        } else {
            throw std::runtime_error(where + ": unknown qos policy '" + key +
                                     "' (expected reliability|history|deadline_ms|liveliness|durability)");
        }
    }
    return p;
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

        // Optional. When absent the profile stays empty and every downstream
        // check short-circuits, leaving the submitted code path untouched.
        if (m.contains("qos_policies")) {
            // A per-leg shape was tried and dropped. Reject it explicitly
            // rather than reading {"source": {...}} as a policy named "source"
            // and throwing an unhelpful message about an unknown policy.
            const auto& qp = m["qos_policies"];
            if (qp.is_object() && (qp.contains("source") || qp.contains("sink"))) {
                throw std::runtime_error("mapping[" + id +
                                         "]: qos_policies is declared once for the mapping, not per leg. "
                                         "Remove the 'source'/'sink' nesting; what each (policy, direction) builds is fixed "
                                         "(common/qos_spec.hpp) and reported at startup");
            }
            cmd.qos_policies = parseQosProfile(qp, "mapping[" + id + "].qos_policies");
        }

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
