#ifndef DDS_PUBLISHER_HPP
#define DDS_PUBLISHER_HPP

#include <fastdds/dds/domain/DomainParticipant.hpp>
#include <fastdds/dds/publisher/DataWriter.hpp>
#include <fastdds/dds/publisher/Publisher.hpp>
#include <fastdds/dds/publisher/qos/DataWriterQos.hpp>
#include <fastdds/dds/topic/Topic.hpp>
#include <fastdds/dds/topic/TypeSupport.hpp>
#include <fastdds/rtps/common/SerializedPayload.h>
#include <spdlog/spdlog.h>

#include <atomic>
#include <chrono>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>

#include "common/command.hpp"
#include "common/conn_key_util.hpp"
#include "common/connection_queue.hpp"
#include "common/resource_tracker.hpp"
#include "ros2_msg_list.h"
#include "ros2_msg_type_registry.hpp"

// One DDS DataWriter per bridge, driven by a dedicated worker thread that
// pulls payloads off the per-bridge ConnectionQueue.
class DDSPublisher {
public:
    DDSPublisher(eprosima::fastdds::dds::DomainParticipant* participant, std::shared_ptr<DDSResourceTracker> tracker,
                 ConnectionQueueMap* queue_map);
    ~DDSPublisher();

    bool handleCreate(const Command& cmd);
    bool handleDelete(const Command& cmd);
    void stop();

private:
    struct WorkerThread {
        std::thread thread;
        std::shared_ptr<std::atomic<bool>> stop_flag;
    };

    void publisherWorker(Command cmd, std::shared_ptr<std::atomic<bool>> stop_flag);
    std::string qosToString(Qos qos);

    std::shared_ptr<spdlog::logger> log_;

    eprosima::fastdds::dds::DomainParticipant* participant_{nullptr};
    eprosima::fastdds::dds::Publisher* publisher_{nullptr};

    std::shared_ptr<DDSResourceTracker> tracker_;
    ConnectionQueueMap* queue_map_;

    std::atomic<bool> running_{true};
    std::mutex resource_mutex_;

    std::map<std::string, eprosima::fastdds::dds::DataWriter*> writers_;
    std::map<std::string, std::unique_ptr<WorkerThread>> worker_threads_;
};

#endif  // DDS_PUBLISHER_HPP
