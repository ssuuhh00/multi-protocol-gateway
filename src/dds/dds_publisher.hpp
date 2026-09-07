#ifndef DDS_PUBLISHER_HPP
#define DDS_PUBLISHER_HPP

#include <fastdds/dds/domain/DomainParticipant.hpp>
#include <fastdds/dds/publisher/DataWriter.hpp>
#include <fastdds/dds/publisher/DataWriterListener.hpp>
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

    /**
     * @brief Reports what happens to the sink leg's QoS contract.
     *
     * The gateway used to create its DataWriter without a listener, which was
     * fine while the only policy was reliability: a mismatch there just meant
     * the reader never appeared. Once a mapping can offer a deadline or a
     * liveliness lease, the failures become invisible without these callbacks --
     * a reader that rejected our offer is indistinguishable from a reader that
     * never started.
     */
    class WriterListener : public eprosima::fastdds::dds::DataWriterListener {
    public:
        WriterListener(std::string conn_key, std::shared_ptr<spdlog::logger> logger)
            : conn_key_(std::move(conn_key)), log_(std::move(logger)) {}

        void on_offered_incompatible_qos(eprosima::fastdds::dds::DataWriter* writer,
                                         const eprosima::fastdds::dds::OfferedIncompatibleQosStatus& status) override;
        void on_offered_deadline_missed(eprosima::fastdds::dds::DataWriter* writer,
                                        const eprosima::fastdds::dds::OfferedDeadlineMissedStatus& status) override;
        void on_liveliness_lost(eprosima::fastdds::dds::DataWriter* writer,
                                const eprosima::fastdds::dds::LivelinessLostStatus& status) override;

    private:
        std::string conn_key_;
        std::shared_ptr<spdlog::logger> log_;
    };

    std::string qosToString(Qos qos);

    std::shared_ptr<spdlog::logger> log_;

    eprosima::fastdds::dds::DomainParticipant* participant_{nullptr};
    eprosima::fastdds::dds::Publisher* publisher_{nullptr};

    std::shared_ptr<DDSResourceTracker> tracker_;
    ConnectionQueueMap* queue_map_;

    std::atomic<bool> running_{true};
    std::mutex resource_mutex_;

    std::map<std::string, eprosima::fastdds::dds::DataWriter*> writers_;
    std::map<std::string, std::shared_ptr<WriterListener>> listeners_;
    std::map<std::string, std::unique_ptr<WorkerThread>> worker_threads_;
};

#endif  // DDS_PUBLISHER_HPP
