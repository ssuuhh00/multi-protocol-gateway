#ifndef DDS_SUBSCRIBER_HPP
#define DDS_SUBSCRIBER_HPP

#include <fastdds/dds/domain/DomainParticipant.hpp>
#include <fastdds/dds/subscriber/DataReader.hpp>
#include <fastdds/dds/subscriber/DataReaderListener.hpp>
#include <fastdds/dds/subscriber/SampleInfo.hpp>
#include <fastdds/dds/subscriber/Subscriber.hpp>
#include <fastdds/dds/subscriber/qos/DataReaderQos.hpp>
#include <fastdds/dds/topic/Topic.hpp>
#include <fastdds/dds/topic/TypeSupport.hpp>
#include <fastdds/rtps/common/SerializedPayload.h>
#include <spdlog/spdlog.h>

#include <atomic>
#include <map>
#include <memory>
#include <mutex>
#include <string>

#include "common/command.hpp"
#include "common/conn_key_util.hpp"
#include "common/connection_queue.hpp"
#include "common/resource_tracker.hpp"
#include "ros2_msg_list.h"
#include "ros2_msg_type_registry.hpp"

// One DDS DataReader per bridge; the listener pushes deserialized payloads
// into the bridge's ConnectionQueue for the sink-side worker to consume.
class DDSSubscriber {
public:
    DDSSubscriber(eprosima::fastdds::dds::DomainParticipant* participant, std::shared_ptr<DDSResourceTracker> tracker,
                  ConnectionQueueMap* queue_map);
    ~DDSSubscriber();

    bool handleCreate(const Command& cmd);
    bool handleDelete(const Command& cmd);
    void stop();

private:
    class ReaderListener : public eprosima::fastdds::dds::DataReaderListener {
    public:
        ReaderListener(ConnectionQueueMap* queue_map, const Command& cmd,
                       eprosima::fastdds::dds::TypeSupport type_support, std::shared_ptr<spdlog::logger> logger);

        void on_data_available(eprosima::fastdds::dds::DataReader* reader) override;
        void on_subscription_matched(eprosima::fastdds::dds::DataReader* reader,
                                     const eprosima::fastdds::dds::SubscriptionMatchedStatus& info) override;

    private:
        ConnectionQueueMap* queue_map_;
        std::string conn_key_;
        std::string topic_name_;
        MessageType message_type_;
        bool bench_mode_;
        eprosima::fastdds::dds::TypeSupport type_support_;
        std::shared_ptr<spdlog::logger> log_;
    };

    std::string qosToString(Qos qos);

    std::shared_ptr<spdlog::logger> log_;

    eprosima::fastdds::dds::DomainParticipant* participant_{nullptr};
    eprosima::fastdds::dds::Subscriber* subscriber_{nullptr};

    std::shared_ptr<DDSResourceTracker> tracker_;
    ConnectionQueueMap* queue_map_;

    std::atomic<bool> running_{true};
    std::mutex resource_mutex_;

    std::map<std::string, eprosima::fastdds::dds::DataReader*> readers_;
    std::map<std::string, std::shared_ptr<ReaderListener>> listeners_;
};

#endif  // DDS_SUBSCRIBER_HPP
