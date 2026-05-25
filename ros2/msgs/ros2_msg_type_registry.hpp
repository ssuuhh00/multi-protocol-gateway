#ifndef ROS2_MSG_TYPE_REGISTRY_HPP
#define ROS2_MSG_TYPE_REGISTRY_HPP

/**
 * @file ros2_msg_type_registry.hpp
 * @brief Auto-generated message type registry.
 * 
 * DO NOT EDIT MANUALLY.
 * Run 'ros2/scripts/update_with_config.sh' to update.
 */

#include <spdlog/spdlog.h>

#include <fastdds/dds/domain/DomainParticipant.hpp>
#include <fastdds/dds/topic/TypeSupport.hpp>

// Generated Headers
#include "std_msgs/msg/StringPubSubTypes.h"

// Auto-generated MessageType Enum
enum class MessageType {
    NONE = 0,
    STD_MSGS_MSG_STRING
};

class DdsTypeRegistry {
public:
    static int registerAllTypes(eprosima::fastdds::dds::DomainParticipant* participant, std::shared_ptr<spdlog::logger> log) {
        int count = 0;

    try {
        eprosima::fastdds::dds::TypeSupport type(new std_msgs::msg::StringPubSubType());
        type.register_type(participant);
        log->debug("  - Registered: std_msgs/msg/String ({})", type.get_type_name());
        count++;
    } catch (const std::exception& e) {
        log->error("  - Failed to register std_msgs/String: {}", e.what());
    }
        return count;
    }

    static std::string getTypeName(MessageType type) {
        switch(type) {
        case MessageType::STD_MSGS_MSG_STRING: return "std_msgs::msg::dds_::String_";
            default: return "";
        }
    }

    static eprosima::fastdds::dds::TypeSupport createTypeSupport(MessageType type) {
        switch(type) {
        case MessageType::STD_MSGS_MSG_STRING: return eprosima::fastdds::dds::TypeSupport(new std_msgs::msg::StringPubSubType());
            default: return eprosima::fastdds::dds::TypeSupport(nullptr);
        }
    }

    static std::string enumToString(MessageType type) {
        switch(type) {
            case MessageType::STD_MSGS_MSG_STRING: return "STD_MSGS_MSG_STRING";
            default: return "UNKNOWN";
        }
    }

    // Returns MessageType::NONE if the string does not match any enabled message.
    static MessageType stringToEnum(const std::string& name) {
        if (name == "STD_MSGS_MSG_STRING") return MessageType::STD_MSGS_MSG_STRING;
        return MessageType::NONE;
    }
};

#endif // ROS2_MSG_TYPE_REGISTRY_HPP
