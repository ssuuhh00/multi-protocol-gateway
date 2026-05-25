#include "dds_subscriber.hpp"

#include <spdlog/sinks/stdout_color_sinks.h>

#include "common/bench_util.hpp"
#include "ros2_msg_data_converter.hpp"

DDSSubscriber::DDSSubscriber(eprosima::fastdds::dds::DomainParticipant* participant,
                             std::shared_ptr<DDSResourceTracker> tracker, ConnectionQueueMap* queue_map)
    : participant_(participant), tracker_(tracker), queue_map_(queue_map) {
    log_ = spdlog::get("DDS_SUB");
    if (!log_) {
        log_ = spdlog::stdout_color_mt("DDS_SUB");
        log_->set_level(spdlog::level::info);
    }

    if (!participant_) {
        log_->critical("FATAL: Injected DomainParticipant is nullptr");
        throw std::runtime_error("DDSSubscriber received null participant");
    }

    subscriber_ = participant_->create_subscriber(eprosima::fastdds::dds::SUBSCRIBER_QOS_DEFAULT, nullptr);
    if (!subscriber_) {
        log_->critical("FATAL: Failed to create Subscriber");
        throw std::runtime_error("Subscriber creation failed");
    }
}

DDSSubscriber::~DDSSubscriber() { stop(); }

void DDSSubscriber::stop() {
    if (!running_.exchange(false)) return;

    // Do NOT delete the participant — owned by GatewayCore.
    if (participant_ && subscriber_) {
        for (auto& [key, reader] : readers_) {
            if (!reader) continue;
            auto topic_desc = reader->get_topicdescription();
            std::string topic_name = topic_desc ? topic_desc->get_name() : "";

            subscriber_->delete_datareader(reader);

            if (!topic_name.empty()) {
                eprosima::fastdds::dds::Topic* topic_to_delete = nullptr;
                int remaining_refs = tracker_->removeTopicRef(topic_name, topic_to_delete);
                if (remaining_refs <= 0 && topic_to_delete) {
                    participant_->delete_topic(topic_to_delete);
                }
            }
        }
        readers_.clear();
        listeners_.clear();

        participant_->delete_subscriber(subscriber_);
        subscriber_ = nullptr;
    }
}

bool DDSSubscriber::handleCreate(const Command& cmd) {
    std::string conn_key = make_conn_key(cmd);
    std::string topic_name = cmd.topic;

    std::lock_guard<std::mutex> lock(resource_mutex_);

    if (readers_.count(conn_key)) {
        log_->warn("Creating: SKIPPED: Reader already exists for key '{}'", conn_key);
        return false;
    }

    queue_map_->create(conn_key);

    std::string type_name = DdsTypeRegistry::getTypeName(cmd.message_type);
    if (type_name.empty()) {
        log_->error("Creating: Unknown message type");
        return false;
    }

    eprosima::fastdds::dds::TypeSupport type_support = DdsTypeRegistry::createTypeSupport(cmd.message_type);
    if (!type_support) {
        log_->error("Creating: Failed to create TypeSupport object");
        return false;
    }

    eprosima::fastdds::dds::TopicQos topic_qos = eprosima::fastdds::dds::TOPIC_QOS_DEFAULT;
    eprosima::fastdds::dds::DataReaderQos reader_qos = eprosima::fastdds::dds::DATAREADER_QOS_DEFAULT;
    if (cmd.qos == Qos::BEST_EFFORT) {
        topic_qos.reliability().kind = eprosima::fastdds::dds::BEST_EFFORT_RELIABILITY_QOS;
        reader_qos.reliability().kind = eprosima::fastdds::dds::BEST_EFFORT_RELIABILITY_QOS;
    } else {
        topic_qos.reliability().kind = eprosima::fastdds::dds::RELIABLE_RELIABILITY_QOS;
        reader_qos.reliability().kind = eprosima::fastdds::dds::RELIABLE_RELIABILITY_QOS;
    }
    reader_qos.history().depth = 10;

    eprosima::fastdds::dds::Topic* topic = tracker_->getTopicAndAddRef(topic_name);
    if (topic != nullptr) {
        tracker_->addSubTopic(topic_name);
    } else {
        if (auto* topic_desc = participant_->lookup_topicdescription(topic_name)) {
            topic = dynamic_cast<eprosima::fastdds::dds::Topic*>(topic_desc);
            if (topic == nullptr) {
                log_->error("Creating: FAILED: Could not cast TopicDescription to Topic for '{}'", topic_name);
                return false;
            }
            tracker_->registerTopic(topic_name, topic);
            tracker_->addSubTopic(topic_name);
        } else {
            topic = participant_->create_topic(topic_name, type_name, topic_qos);
            if (topic == nullptr) {
                log_->error("Creating: FAILED: Could not create topic '{}' (check type registration)", topic_name);
                return false;
            }
            tracker_->registerTopic(topic_name, topic);
            tracker_->addSubTopic(topic_name);
        }
    }

    auto listener = std::make_shared<ReaderListener>(queue_map_, cmd, type_support, log_);
    listeners_[conn_key] = listener;

    eprosima::fastdds::dds::DataReader* reader = subscriber_->create_datareader(topic, reader_qos, listener.get());
    if (reader == nullptr) {
        log_->error("Creating: FAILED: Could not create DataReader for '{}'", topic_name);
        listeners_.erase(conn_key);
        return false;
    }

    readers_[conn_key] = reader;
    log_->debug("Creating: SUCCESS: Reader started for '{}' (key: {})", topic_name, conn_key);
    return true;
}

bool DDSSubscriber::handleDelete(const Command& cmd) {
    std::string conn_key = make_conn_key(cmd);
    std::string topic_name = cmd.topic;

    std::lock_guard<std::mutex> lock(resource_mutex_);

    if (!readers_.count(conn_key)) {
        log_->warn("Deleting: SKIPPED: No reader found for key '{}'", conn_key);
        return false;
    }

    subscriber_->delete_datareader(readers_[conn_key]);
    readers_.erase(conn_key);
    listeners_.erase(conn_key);

    tracker_->removeSubTopic(topic_name);

    eprosima::fastdds::dds::Topic* topic_to_delete = nullptr;
    int remaining_refs = tracker_->removeTopicRef(topic_name, topic_to_delete);
    if (remaining_refs <= 0 && topic_to_delete) {
        participant_->delete_topic(topic_to_delete);
    }

    log_->debug("Deleting: SUCCESS: Cleaned up key '{}'", conn_key);
    return true;
}

DDSSubscriber::ReaderListener::ReaderListener(ConnectionQueueMap* queue_map, const Command& cmd,
                                              eprosima::fastdds::dds::TypeSupport type_support,
                                              std::shared_ptr<spdlog::logger> logger)
    : queue_map_(queue_map),
      message_type_(cmd.message_type),
      bench_mode_(cmd.bench_mode),
      type_support_(type_support),
      log_(logger) {
    conn_key_ = make_conn_key(cmd);
    topic_name_ = cmd.topic;
}

void DDSSubscriber::ReaderListener::on_data_available(eprosima::fastdds::dds::DataReader* reader) {
    void* data = type_support_.get()->createData();
    eprosima::fastdds::dds::SampleInfo info;

    if (reader->take_next_sample(data, &info) == eprosima::fastrtps::types::ReturnCode_t::RETCODE_OK) {
        if (info.valid_data) {
            auto t_conv_start = std::chrono::steady_clock::now();
            std::vector<uint8_t> out_bytes = DataConverter::msgToPayload(message_type_, data);
            bool conv_ok = !out_bytes.empty();
            auto t_bytes_ready = std::chrono::steady_clock::now();
            auto conv_latency_us =
                std::chrono::duration_cast<std::chrono::microseconds>(t_bytes_ready - t_conv_start).count();

            if (conv_ok) {
                // Read bench header BEFORE the queue->push() moves out_bytes.
                uint64_t bench_seq_key = 0;
                int64_t bench_src_mw_lumped_us = 0;
                if (bench_mode_) {
                    bench_seq_key = bench::read_slot(out_bytes, bench::Slot::SEQ_KEY);
                    const int64_t t_pub_send = static_cast<int64_t>(bench::read_slot(out_bytes, bench::Slot::TS));
                    bench_src_mw_lumped_us = (t_bytes_ready.time_since_epoch().count() - t_pub_send) / 1000;
                }

                if (auto queue = queue_map_->get(conn_key_)) {
                    size_t data_size = out_bytes.size();
                    queue->push(std::move(out_bytes));

                    if (bench_mode_) {
                        log_->trace("[Callback:{}] | Seq: {} | SrcMW: {} us | Conv: {} us | Size: {} B",
                                    topic_name_, bench_seq_key, bench_src_mw_lumped_us, conv_latency_us, data_size);
                    } else {
                        log_->trace("[Callback:{}] | Conv: {} us | Size: {} B", topic_name_, conv_latency_us,
                                    data_size);
                    }
                } else {
                    log_->error("[Callback:{}] Queue not found for key '{}' - data dropped!", topic_name_, conn_key_);
                }
            } else {
                log_->error("[Callback:{}] Conversion failed!", topic_name_);
            }
        }
    }

    type_support_.get()->deleteData(data);
}

void DDSSubscriber::ReaderListener::on_subscription_matched(
    eprosima::fastdds::dds::DataReader* /*reader*/, const eprosima::fastdds::dds::SubscriptionMatchedStatus& info) {
    if (info.current_count_change > 0) {
        log_->debug("[Match:{}] Publisher matched (total: {})", topic_name_, info.current_count);
    } else if (info.current_count_change < 0) {
        log_->debug("[Match:{}] Publisher unmatched (total: {})", topic_name_, info.current_count);
    }
}

std::string DDSSubscriber::qosToString(Qos qos) {
    switch (qos) {
        case Qos::RELIABLE:    return "RELIABLE";
        case Qos::BEST_EFFORT: return "BEST_EFFORT";
        default:               return "DEFAULT";
    }
}
