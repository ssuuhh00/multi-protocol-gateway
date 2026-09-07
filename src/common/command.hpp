#ifndef COMMON_COMMAND_HPP
#define COMMON_COMMAND_HPP

#include <cstdint>
#include <optional>
#include <string>

#include "common/qos_profile.hpp"
#include "ros2_msg_type_registry.hpp"

// source = side that reads external data (DDS sub / SOME/IP client / Zenoh sub).
// sink   = side that writes outward     (DDS pub / SOME/IP server / Zenoh pub).
enum class ProtocolType { NONE = 0, DDS, SOMEIP, ZENOH };
enum class Qos { NONE = 0, RELIABLE, BEST_EFFORT };

// Command passed to GatewayCore::createBridge() / deleteBridge(). The protocol-
// specific address fields are populated only for the side(s) that use them.
struct Command {
    ProtocolType source = ProtocolType::NONE;
    ProtocolType sink = ProtocolType::NONE;

    MessageType message_type = MessageType::NONE;
    Qos qos = Qos::NONE;

    // Fine-grained QoS for the mapping. One profile, not one per leg: the
    // declaration describes the data crossing the bridge, and the bridge is a
    // single path. What each leg is built with is fixed per (policy, direction)
    // by the spec sheet (common/qos_spec.hpp) and printed at startup, rather
    // than restated by the operator per leg.
    //
    // Empty means "not declared": the gateway then leaves every middleware knob
    // at its default. `qos` above is untouched and still drives the reliability
    // of both legs when no profile is given, so an existing mapping file
    // behaves exactly as before.
    QosProfile qos_policies;

    std::optional<uint16_t> service_id;
    std::optional<uint16_t> instance_id;
    std::optional<uint16_t> event_id;
    std::optional<uint16_t> eventgroup_id;

    std::string topic;
    std::string zenoh_key;

    bool bench_mode = false;

    void reset() { *this = Command{}; }
};

#endif  // COMMON_COMMAND_HPP
