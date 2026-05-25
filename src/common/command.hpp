#ifndef COMMON_COMMAND_HPP
#define COMMON_COMMAND_HPP

#include <cstdint>
#include <optional>
#include <string>

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
