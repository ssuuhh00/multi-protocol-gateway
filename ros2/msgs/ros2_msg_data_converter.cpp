#include "ros2_msg_data_converter.hpp"

#include <fastrtps/rtps/common/SerializedPayload.h>
#include <fastrtps/rtps/common/Types.h>

// --- std_msgs::msg::String ---

bool payloadToMsg_std_msgs_msg_String(const std::vector<uint8_t>& payload, void* msg) {
    auto* string_msg = static_cast<std_msgs::msg::String*>(msg);
    string_msg->data(std::string(payload.begin(), payload.end()));
    return true;
}

std::vector<uint8_t> msgToPayload_std_msgs_msg_String(void* msg) {
    auto* string_msg = static_cast<std_msgs::msg::String*>(msg);
    std::vector<uint8_t> payload;
    const auto& str = string_msg->data();
    payload = std::vector<uint8_t>(str.begin(), str.end());
    return payload;
}