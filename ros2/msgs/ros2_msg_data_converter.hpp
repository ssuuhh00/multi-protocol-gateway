#ifndef ROS2_MSG_DATA_CONVERTER_HPP
#define ROS2_MSG_DATA_CONVERTER_HPP

/**
 * @file ros2_msg_data_converter.hpp
 * @brief Logic for converting between SOMEIP/Zenoh payload and ROS 2 msg.
 * 
 * This header contains declarations only. Implement these functions in ros2_msg_data_converter.cpp
 * Run 'ros2/scripts/update_with_config.sh' to update this header.
 */

#include <cstring>
#include <string>
#include <vector>

#include "ros2_msg_list.h"
#include "ros2_msg_type_registry.hpp"

/**
 * @brief payloadToMsg: SOMEIP/Zenoh received payload -> Populate ROS2 msg fields (DDS_PUB path)
 * @brief msgToPayload: ROS2 msg fields -> Convert to SOMEIP/Zenoh send payload (DDS_SUB path)
 * 
 * payload = Raw byte data sent via SOMEIP/Zenoh protocol
 *           This is the raw payload created by the sender, not an internal gateway format.
 */

// --- User Implemented Conversion Functions ---

bool payloadToMsg_std_msgs_msg_String(const std::vector<uint8_t>& payload, void* msg);
std::vector<uint8_t> msgToPayload_std_msgs_msg_String(void* msg);

/**
 * @class DataConverter
 * @brief Dispatcher class for mapping payload conversions.
 */

class DataConverter {
public:
    // DDS_PUB path: SOMEIP/Zenoh payload -> ROS2 msg fields
    // Returns false if conversion fails
    static bool payloadToMsg(MessageType type, const std::vector<uint8_t>& payload, void* msg) {
        switch (type) {
            case MessageType::STD_MSGS_MSG_STRING: return payloadToMsg_std_msgs_msg_String(payload, msg);
            default: return false;
        }
    }

    // DDS_SUB path: ROS2 msg fields -> SOMEIP/Zenoh payload
    // Returns empty vector if conversion fails
    static std::vector<uint8_t> msgToPayload(MessageType type, void* msg) {
        switch (type) {
            case MessageType::STD_MSGS_MSG_STRING: return msgToPayload_std_msgs_msg_String(msg);
            default: return {};
        }
    }
};

#endif // ROS2_MSG_DATA_CONVERTER_HPP
