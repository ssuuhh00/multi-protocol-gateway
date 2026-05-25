#ifndef EXTERNAL_EXT_DDS_PUB_HPP
#define EXTERNAL_EXT_DDS_PUB_HPP

#include <fastdds/dds/domain/DomainParticipant.hpp>
#include <fastdds/dds/publisher/DataWriter.hpp>
#include <fastdds/dds/publisher/Publisher.hpp>
#include <fastdds/dds/topic/Topic.hpp>
#include <fastdds/dds/topic/TypeSupport.hpp>

#include <string>

#include "common/command.hpp"
#include "ext_node.hpp"

class ExtDdsPub : public ExtPublisher {
public:
    ExtDdsPub(eprosima::fastdds::dds::DomainParticipant* participant, std::string topic, MessageType mt, Qos qos);
    ~ExtDdsPub() override;

    void start() override;
    void stop() override;
    void send(uint64_t seq_key, size_t payload_bytes) override;

private:
    eprosima::fastdds::dds::DomainParticipant* participant_ = nullptr;
    std::string topic_name_;
    MessageType message_type_;
    Qos qos_;

    eprosima::fastdds::dds::TypeSupport type_support_;
    eprosima::fastdds::dds::Publisher* publisher_ = nullptr;
    eprosima::fastdds::dds::Topic* topic_ = nullptr;
    eprosima::fastdds::dds::DataWriter* writer_ = nullptr;
    void* msg_object_ = nullptr;
    bool started_ = false;
};

#endif  // EXTERNAL_EXT_DDS_PUB_HPP
