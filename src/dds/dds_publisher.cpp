#include "dds_publisher.hpp"

#include <spdlog/sinks/stdout_color_sinks.h>

#include <cstring>

#include "common/bench_util.hpp"
#include "ros2_msg_data_converter.hpp"

DDSPublisher::DDSPublisher(eprosima::fastdds::dds::DomainParticipant* participant,
                           std::shared_ptr<DDSResourceTracker> tracker, ConnectionQueueMap* queue_map)
    : participant_(participant), tracker_(tracker), queue_map_(queue_map) {
    log_ = spdlog::get("DDS_PUB");
    if (!log_) {
        log_ = spdlog::stdout_color_mt("DDS_PUB");
        log_->set_level(spdlog::level::info);
    }

    if (!participant_) {
        log_->critical("FATAL: Injected DomainParticipant is nullptr");
        throw std::runtime_error("DDSPublisher received null participant");
    }

    publisher_ = participant_->create_publisher(eprosima::fastdds::dds::PUBLISHER_QOS_DEFAULT, nullptr);
    if (!publisher_) {
        log_->critical("FATAL: Failed to create Publisher");
        throw std::runtime_error("Publisher creation failed");
    }
}

DDSPublisher::~DDSPublisher() { stop(); }

void DDSPublisher::stop() {
    if (!running_.exchange(false)) return;

    {
        std::lock_guard<std::mutex> lock(resource_mutex_);
        for (auto& [key, worker] : worker_threads_) {
            if (!worker) continue;
            worker->stop_flag->store(true);
            // Wake the blocked pop() with a dummy push (poison pill).
            if (auto q = queue_map_->get(key)) {
                q->push(std::vector<uint8_t>{});
            }
            if (worker->thread.joinable()) {
                worker->thread.join();
            }
        }
        worker_threads_.clear();
    }

    // Do NOT delete the participant — owned by GatewayCore.
    if (participant_ && publisher_) {
        for (auto& [key, writer] : writers_) {
            if (!writer) continue;
            auto writer_topic = writer->get_topic();
            std::string topic_name = writer_topic ? writer_topic->get_name() : "";

            publisher_->delete_datawriter(writer);

            if (!topic_name.empty()) {
                eprosima::fastdds::dds::Topic* topic_to_delete = nullptr;
                int remaining_refs = tracker_->removeTopicRef(topic_name, topic_to_delete);
                if (remaining_refs <= 0 && topic_to_delete) {
                    participant_->delete_topic(topic_to_delete);
                }
            }
        }
        writers_.clear();

        participant_->delete_publisher(publisher_);
        publisher_ = nullptr;
    }
}

bool DDSPublisher::handleCreate(const Command& cmd) {
    std::string conn_key = make_conn_key(cmd);
    std::string topic_name = cmd.topic;

    std::lock_guard<std::mutex> lock(resource_mutex_);

    if (worker_threads_.count(conn_key)) {
        log_->warn("Creating: SKIPPED: Connection already exists for key '{}'", conn_key);
        return false;
    }

    queue_map_->create(conn_key);

    std::string type_name = DdsTypeRegistry::getTypeName(cmd.message_type);
    if (type_name.empty()) {
        log_->error("Creating: Unknown message type");
        return false;
    }

    eprosima::fastdds::dds::TopicQos topic_qos = eprosima::fastdds::dds::TOPIC_QOS_DEFAULT;
    eprosima::fastdds::dds::DataWriterQos writer_qos = eprosima::fastdds::dds::DATAWRITER_QOS_DEFAULT;
    if (cmd.qos == Qos::BEST_EFFORT) {
        topic_qos.reliability().kind = eprosima::fastdds::dds::BEST_EFFORT_RELIABILITY_QOS;
        writer_qos.reliability().kind = eprosima::fastdds::dds::BEST_EFFORT_RELIABILITY_QOS;
    } else {
        topic_qos.reliability().kind = eprosima::fastdds::dds::RELIABLE_RELIABILITY_QOS;
        writer_qos.reliability().kind = eprosima::fastdds::dds::RELIABLE_RELIABILITY_QOS;
    }
    writer_qos.history().depth = 10;

    eprosima::fastdds::dds::Topic* topic = tracker_->getTopicAndAddRef(topic_name);
    if (topic == nullptr) {
        // Topic might already exist on the participant (e.g. created by a
        // co-located DDS app); only call create_topic if not.
        if (auto* topic_desc = participant_->lookup_topicdescription(topic_name)) {
            topic = dynamic_cast<eprosima::fastdds::dds::Topic*>(topic_desc);
            if (topic == nullptr) {
                log_->error("Creating: FAILED: Could not cast TopicDescription to Topic for '{}'", topic_name);
                return false;
            }
            tracker_->registerTopic(topic_name, topic);
        } else {
            topic = participant_->create_topic(topic_name, type_name, topic_qos);
            if (topic == nullptr) {
                log_->error("Creating: FAILED: Could not create topic '{}' (check type registration)", topic_name);
                return false;
            }
            tracker_->registerTopic(topic_name, topic);
        }
    }

    eprosima::fastdds::dds::DataWriter* writer = publisher_->create_datawriter(topic, writer_qos);
    if (writer == nullptr) {
        log_->error("Creating: FAILED: Could not create DataWriter for '{}'", topic_name);
        return false;
    }

    writers_[conn_key] = writer;
    tracker_->addPubTopic(topic_name);

    auto worker = std::make_unique<WorkerThread>();
    worker->stop_flag = std::make_shared<std::atomic<bool>>(false);
    worker->thread = std::thread(&DDSPublisher::publisherWorker, this, cmd, worker->stop_flag);
    worker_threads_[conn_key] = std::move(worker);

    log_->debug("Creating: SUCCESS: Writer and worker started for '{}' (key: {})", topic_name, conn_key);
    return true;
}

bool DDSPublisher::handleDelete(const Command& cmd) {
    std::string conn_key = make_conn_key(cmd);
    std::string topic_name = cmd.topic;

    std::lock_guard<std::mutex> lock(resource_mutex_);

    if (!worker_threads_.count(conn_key)) {
        log_->warn("Deleting: SKIPPED: No worker found for key '{}'", conn_key);
        return false;
    }

    worker_threads_[conn_key]->stop_flag->store(true);
    if (auto queue = queue_map_->get(conn_key)) {
        queue->push(std::vector<uint8_t>{});
    }
    if (worker_threads_[conn_key]->thread.joinable()) {
        worker_threads_[conn_key]->thread.join();
    }
    worker_threads_.erase(conn_key);

    if (writers_.count(conn_key)) {
        publisher_->delete_datawriter(writers_[conn_key]);
        writers_.erase(conn_key);
    }

    eprosima::fastdds::dds::Topic* topic_to_delete = nullptr;
    int remaining_refs = tracker_->removeTopicRef(topic_name, topic_to_delete);
    if (remaining_refs <= 0 && topic_to_delete) {
        participant_->delete_topic(topic_to_delete);
    }
    tracker_->removePubTopic(topic_name);

    queue_map_->remove(conn_key);

    log_->debug("Deleting: SUCCESS: Cleaned up key '{}'", conn_key);
    return true;
}

void DDSPublisher::publisherWorker(Command cmd, std::shared_ptr<std::atomic<bool>> stop_flag) {
    std::string conn_key = make_conn_key(cmd);
    std::string topic_name = cmd.topic;

    auto data_queue = queue_map_->get(conn_key);
    if (!data_queue) {
        log_->error("[Worker:{}] FAILED: Queue is nullptr for key '{}'", topic_name, conn_key);
        return;
    }

    eprosima::fastdds::dds::DataWriter* writer = nullptr;
    {
        std::lock_guard<std::mutex> lock(resource_mutex_);
        auto it = writers_.find(conn_key);
        if (it != writers_.end()) writer = it->second;
    }
    if (!writer) {
        log_->error("[Worker:{}] FAILED: DataWriter not found for key '{}'", topic_name, conn_key);
        return;
    }

    eprosima::fastdds::dds::TypeSupport type_support = DdsTypeRegistry::createTypeSupport(cmd.message_type);
    if (!type_support) {
        log_->error("[Worker:{}] FAILED: TypeSupport creation failed", topic_name);
        return;
    }

    void* msg_object = type_support.get()->createData();
    if (!msg_object) {
        log_->error("[Worker:{}] FAILED: Could not create message object", topic_name);
        return;
    }

    while (running_ && !stop_flag->load()) {
        DataType data_box = data_queue->pop();

        if (!running_ || stop_flag->load() || data_box.data.empty()) break;

        // Bench: stamp T_BYTES_LEAVE in the TS slot so ext_sub can compute
        // sink_mw_lumped. queue_us is captured now too.
        auto t_bytes_leave = std::chrono::steady_clock::now();
        if (cmd.bench_mode) {
            bench::stamp_value(data_box.data, bench::Slot::TS, t_bytes_leave.time_since_epoch().count());
        }
        auto queue_latency_us =
            std::chrono::duration_cast<std::chrono::microseconds>(t_bytes_leave - data_box.timestamp).count();

        auto t_conv_start = std::chrono::steady_clock::now();
        bool conv_ok = DataConverter::payloadToMsg(cmd.message_type, data_box.data, msg_object);
        auto t_conv_end = std::chrono::steady_clock::now();
        auto conv_latency_us = std::chrono::duration_cast<std::chrono::microseconds>(t_conv_end - t_conv_start).count();

        if (!conv_ok) {
            log_->error("[Worker:{}] Conversion failed (size: {} bytes)", topic_name, data_box.data.size());
            continue;
        }

        const uint64_t bench_seq_key =
            cmd.bench_mode ? bench::read_slot(data_box.data, bench::Slot::SEQ_KEY) : 0;

        // Fast DDS v2.14.5 ReturnCode_t operator==/!= are broken (ABI mismatch);
        // operator! works correctly so we use that here.
        auto ret = writer->write(msg_object);
        if (!ret) {
            log_->warn("[Worker:{}] Write FAILED", topic_name);
        }

        if (cmd.bench_mode) {
            log_->trace("[Worker:{}] | Seq: {} | Queue: {} us | Conv: {} us | Size: {} B", topic_name,
                        bench_seq_key, queue_latency_us, conv_latency_us, data_box.data.size());
        } else {
            log_->trace("[Worker:{}] | Queue: {} us | Conv: {} us | Size: {} B", topic_name,
                        queue_latency_us, conv_latency_us, data_box.data.size());
        }
    }

    type_support.get()->deleteData(msg_object);
}

std::string DDSPublisher::qosToString(Qos qos) {
    switch (qos) {
        case Qos::RELIABLE:    return "RELIABLE";
        case Qos::BEST_EFFORT: return "BEST_EFFORT";
        default:               return "DEFAULT";
    }
}
