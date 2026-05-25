#ifndef EXTERNAL_EXT_DDS_SUB_HPP
#define EXTERNAL_EXT_DDS_SUB_HPP

#include <fastdds/dds/domain/DomainParticipant.hpp>
#include <fastdds/dds/subscriber/DataReader.hpp>
#include <fastdds/dds/subscriber/DataReaderListener.hpp>
#include <fastdds/dds/subscriber/Subscriber.hpp>
#include <fastdds/dds/topic/Topic.hpp>
#include <fastdds/dds/topic/TypeSupport.hpp>

#include <string>

#include "common/command.hpp"
#include "ext_node.hpp"

class ExtDdsSub : public ExtSubscriber {
public:
    ExtDdsSub(eprosima::fastdds::dds::DomainParticipant* participant, std::string topic, MessageType mt, Qos qos);
    ~ExtDdsSub() override;

    void start() override;
    void stop() override;

private:
    class ReaderListener : public eprosima::fastdds::dds::DataReaderListener {
    public:
        ReaderListener(ExtDdsSub* owner, eprosima::fastdds::dds::TypeSupport ts, MessageType mt);
        void on_data_available(eprosima::fastdds::dds::DataReader* reader) override;

    private:
        ExtDdsSub* owner_;
        eprosima::fastdds::dds::TypeSupport type_support_;
        MessageType message_type_;
    };

    eprosima::fastdds::dds::DomainParticipant* participant_ = nullptr;
    std::string topic_name_;
    MessageType message_type_;
    Qos qos_;

    eprosima::fastdds::dds::TypeSupport type_support_;
    eprosima::fastdds::dds::Subscriber* subscriber_ = nullptr;
    eprosima::fastdds::dds::Topic* topic_ = nullptr;
    eprosima::fastdds::dds::DataReader* reader_ = nullptr;
    ReaderListener* listener_ = nullptr;
    bool started_ = false;
};

#endif  // EXTERNAL_EXT_DDS_SUB_HPP
