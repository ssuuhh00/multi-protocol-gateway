#include "ext_dds_pub.hpp"

#include <fastdds/dds/publisher/qos/DataWriterQos.hpp>
#include <fastdds/dds/topic/qos/TopicQos.hpp>

#include <chrono>
#include <stdexcept>
#include <utility>

#include <spdlog/spdlog.h>

#include "common/bench_util.hpp"
#include "ros2_msg_data_converter.hpp"
#include "ros2_msg_type_registry.hpp"

ExtDdsPub::ExtDdsPub(eprosima::fastdds::dds::DomainParticipant* participant, std::string topic, MessageType mt,
                     Qos qos)
    : participant_(participant), topic_name_(std::move(topic)), message_type_(mt), qos_(qos) {
    if (!participant_) {
        throw std::runtime_error("ExtDdsPub: null participant");
    }
}

ExtDdsPub::~ExtDdsPub() { stop(); }

void ExtDdsPub::start() {
    if (started_) return;

    type_support_ = DdsTypeRegistry::createTypeSupport(message_type_);
    if (!type_support_) {
        throw std::runtime_error("ExtDdsPub: TypeSupport creation failed for " + topic_name_);
    }

    publisher_ = participant_->create_publisher(eprosima::fastdds::dds::PUBLISHER_QOS_DEFAULT, nullptr);
    if (!publisher_) {
        throw std::runtime_error("ExtDdsPub: create_publisher failed for " + topic_name_);
    }

    eprosima::fastdds::dds::TopicQos topic_qos = eprosima::fastdds::dds::TOPIC_QOS_DEFAULT;
    eprosima::fastdds::dds::DataWriterQos writer_qos = eprosima::fastdds::dds::DATAWRITER_QOS_DEFAULT;
    if (qos_ == Qos::BEST_EFFORT) {
        topic_qos.reliability().kind = eprosima::fastdds::dds::BEST_EFFORT_RELIABILITY_QOS;
        writer_qos.reliability().kind = eprosima::fastdds::dds::BEST_EFFORT_RELIABILITY_QOS;
    } else {
        topic_qos.reliability().kind = eprosima::fastdds::dds::RELIABLE_RELIABILITY_QOS;
        writer_qos.reliability().kind = eprosima::fastdds::dds::RELIABLE_RELIABILITY_QOS;
    }
    writer_qos.history().depth = 10;

    auto* topic_desc = participant_->lookup_topicdescription(topic_name_);
    if (topic_desc) {
        topic_ = dynamic_cast<eprosima::fastdds::dds::Topic*>(topic_desc);
    } else {
        topic_ = participant_->create_topic(topic_name_, type_support_.get_type_name(), topic_qos);
    }
    if (!topic_) {
        throw std::runtime_error("ExtDdsPub: failed to obtain topic '" + topic_name_ + "'");
    }

    writer_ = publisher_->create_datawriter(topic_, writer_qos);
    if (!writer_) {
        throw std::runtime_error("ExtDdsPub: create_datawriter failed for " + topic_name_);
    }

    msg_object_ = type_support_.get()->createData();
    started_ = true;
}

void ExtDdsPub::stop() {
    if (!started_) return;
    started_ = false;

    if (msg_object_ && type_support_) {
        type_support_.get()->deleteData(msg_object_);
        msg_object_ = nullptr;
    }
    if (publisher_ && writer_) {
        publisher_->delete_datawriter(writer_);
        writer_ = nullptr;
    }
    if (publisher_) {
        participant_->delete_publisher(publisher_);
        publisher_ = nullptr;
    }
    // The participant cleans up the Topic when it is destroyed.
    topic_ = nullptr;
}

void ExtDdsPub::send(uint64_t seq_key, size_t payload_bytes) {
    if (!started_) return;

    std::vector<uint8_t> payload(payload_bytes, static_cast<uint8_t>('A'));
    bench::prepend_header(payload, seq_key);

    const auto t0 = std::chrono::steady_clock::now();
    if (!DataConverter::payloadToMsg(message_type_, payload, msg_object_)) {
        throw std::runtime_error("ExtDdsPub: payloadToMsg failed for " + topic_name_);
    }
    const auto t1 = std::chrono::steady_clock::now();
    const int64_t payloadToMsg_us = std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0).count();

    const size_t bench_size = payload.size();
    writer_->write(msg_object_);

    spdlog::get("DDS_PUB")->trace("[Send:{}] | Seq: {} | Conv: {} us | Size: {} B",
                                  topic_name_, seq_key, payloadToMsg_us, bench_size);
}
