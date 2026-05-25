#include "ext_dds_sub.hpp"

#include <fastdds/dds/subscriber/SampleInfo.hpp>
#include <fastdds/dds/subscriber/qos/DataReaderQos.hpp>
#include <fastdds/dds/topic/qos/TopicQos.hpp>

#include <chrono>
#include <stdexcept>
#include <utility>

#include <spdlog/spdlog.h>

#include "common/bench_util.hpp"
#include "ros2_msg_data_converter.hpp"
#include "ros2_msg_type_registry.hpp"

ExtDdsSub::ReaderListener::ReaderListener(ExtDdsSub* owner, eprosima::fastdds::dds::TypeSupport ts, MessageType mt)
    : owner_(owner), type_support_(ts), message_type_(mt) {}

void ExtDdsSub::ReaderListener::on_data_available(eprosima::fastdds::dds::DataReader* reader) {
    void* data = type_support_.get()->createData();
    eprosima::fastdds::dds::SampleInfo info;
    if (reader->take_next_sample(data, &info) == eprosima::fastrtps::types::ReturnCode_t::RETCODE_OK) {
        if (info.valid_data) {
            const auto t_a = std::chrono::steady_clock::now();
            std::vector<uint8_t> payload = DataConverter::msgToPayload(message_type_, data);
            const auto t_bytes_ready = std::chrono::steady_clock::now();
            const int64_t msgToPayload_us =
                std::chrono::duration_cast<std::chrono::microseconds>(t_bytes_ready - t_a).count();

            if (!payload.empty()) {
                const uint64_t seq_key = bench::read_slot(payload, bench::Slot::SEQ_KEY);
                const int64_t t_bytes_leave = static_cast<int64_t>(bench::read_slot(payload, bench::Slot::TS));
                const int64_t sink_mw_lumped_us =
                    (t_bytes_ready.time_since_epoch().count() - t_bytes_leave) / 1000;
                spdlog::get("DDS_SUB")->trace("[Callback:{}] | Seq: {} | SinkMW: {} us | Conv: {} us | Size: {} B",
                                              owner_->topic_name_, seq_key, sink_mw_lumped_us, msgToPayload_us,
                                              payload.size());

                if (owner_->on_message_) {
                    owner_->on_message_(payload, t_bytes_ready.time_since_epoch().count());
                }
            }
        }
    }
    type_support_.get()->deleteData(data);
}

ExtDdsSub::ExtDdsSub(eprosima::fastdds::dds::DomainParticipant* participant, std::string topic, MessageType mt,
                     Qos qos)
    : participant_(participant), topic_name_(std::move(topic)), message_type_(mt), qos_(qos) {
    if (!participant_) {
        throw std::runtime_error("ExtDdsSub: null participant");
    }
}

ExtDdsSub::~ExtDdsSub() { stop(); }

void ExtDdsSub::start() {
    if (started_) return;

    type_support_ = DdsTypeRegistry::createTypeSupport(message_type_);
    if (!type_support_) {
        throw std::runtime_error("ExtDdsSub: TypeSupport creation failed for " + topic_name_);
    }

    subscriber_ = participant_->create_subscriber(eprosima::fastdds::dds::SUBSCRIBER_QOS_DEFAULT, nullptr);
    if (!subscriber_) {
        throw std::runtime_error("ExtDdsSub: create_subscriber failed for " + topic_name_);
    }

    eprosima::fastdds::dds::TopicQos topic_qos = eprosima::fastdds::dds::TOPIC_QOS_DEFAULT;
    eprosima::fastdds::dds::DataReaderQos reader_qos = eprosima::fastdds::dds::DATAREADER_QOS_DEFAULT;
    if (qos_ == Qos::BEST_EFFORT) {
        topic_qos.reliability().kind = eprosima::fastdds::dds::BEST_EFFORT_RELIABILITY_QOS;
        reader_qos.reliability().kind = eprosima::fastdds::dds::BEST_EFFORT_RELIABILITY_QOS;
    } else {
        topic_qos.reliability().kind = eprosima::fastdds::dds::RELIABLE_RELIABILITY_QOS;
        reader_qos.reliability().kind = eprosima::fastdds::dds::RELIABLE_RELIABILITY_QOS;
    }
    reader_qos.history().depth = 10;

    auto* topic_desc = participant_->lookup_topicdescription(topic_name_);
    if (topic_desc) {
        topic_ = dynamic_cast<eprosima::fastdds::dds::Topic*>(topic_desc);
    } else {
        topic_ = participant_->create_topic(topic_name_, type_support_.get_type_name(), topic_qos);
    }
    if (!topic_) {
        throw std::runtime_error("ExtDdsSub: failed to obtain topic '" + topic_name_ + "'");
    }

    listener_ = new ReaderListener(this, type_support_, message_type_);
    reader_ = subscriber_->create_datareader(topic_, reader_qos, listener_);
    if (!reader_) {
        delete listener_;
        listener_ = nullptr;
        throw std::runtime_error("ExtDdsSub: create_datareader failed for " + topic_name_);
    }
    started_ = true;
}

void ExtDdsSub::stop() {
    if (!started_) return;
    started_ = false;

    if (subscriber_ && reader_) {
        subscriber_->delete_datareader(reader_);
        reader_ = nullptr;
    }
    if (subscriber_) {
        participant_->delete_subscriber(subscriber_);
        subscriber_ = nullptr;
    }
    delete listener_;
    listener_ = nullptr;
    topic_ = nullptr;
}
