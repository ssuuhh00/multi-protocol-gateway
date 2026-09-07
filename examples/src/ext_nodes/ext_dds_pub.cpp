#include "ext_dds_pub.hpp"

#include <fastdds/dds/publisher/qos/DataWriterQos.hpp>
#include <fastdds/dds/topic/qos/TopicQos.hpp>

#include <chrono>
#include <stdexcept>
#include <utility>

#include <spdlog/spdlog.h>

#include "common/bench_util.hpp"
#include "dds/dds_qos_apply.hpp"
#include "ros2_msg_data_converter.hpp"
#include "ros2_msg_type_registry.hpp"

namespace efd = eprosima::fastdds::dds;

void ExtDdsPub::WriterListener::on_publication_matched(efd::DataWriter* /*writer*/,
                                                       const efd::PublicationMatchedStatus& info) {
    owner_->matched_.store(static_cast<uint32_t>(info.current_count), std::memory_order_relaxed);
    spdlog::get("DDS_PUB")->debug("[{}] publication matched: {:+d} -> {} reader(s)", owner_->spec_.address,
                                  info.current_count_change, info.current_count);
}

ExtDdsPub::ExtDdsPub(efd::DomainParticipant* participant, EndpointSpec spec, MessageType mt, Qos qos,
                     const QosProfile& profile)
    : ExtPublisher(std::move(spec)), participant_(participant), message_type_(mt), qos_(qos), profile_(profile) {
    if (!participant_) throw std::runtime_error("ExtDdsPub: null participant");
}

ExtDdsPub::~ExtDdsPub() { stop(); }

std::string ExtDdsPub::declared() const {
    std::string s = (qos_ == Qos::BEST_EFFORT) ? "BEST_EFFORT" : "RELIABLE";
    s += " KEEP_LAST(" + std::to_string(history_depth_) + ")";
    return s;
}

void ExtDdsPub::start() {
    if (started_) return;

    type_support_ = DdsTypeRegistry::createTypeSupport(message_type_);
    if (!type_support_) {
        throw std::runtime_error("ExtDdsPub: TypeSupport creation failed for " + spec_.address);
    }

    publisher_ = participant_->create_publisher(efd::PUBLISHER_QOS_DEFAULT, nullptr);
    if (!publisher_) throw std::runtime_error("ExtDdsPub: create_publisher failed for " + spec_.address);

    efd::TopicQos topic_qos = efd::TOPIC_QOS_DEFAULT;
    efd::DataWriterQos writer_qos = efd::DATAWRITER_QOS_DEFAULT;

    const auto kind = (qos_ == Qos::BEST_EFFORT) ? efd::BEST_EFFORT_RELIABILITY_QOS : efd::RELIABLE_RELIABILITY_QOS;
    topic_qos.reliability().kind = kind;
    writer_qos.reliability().kind = kind;
    writer_qos.history().depth = 10;
    // The mapping's declaration, applied to this endpoint too. Without it the
    // endpoint is not a counterpart: on the DDS request/offer rules a writer
    // that offers nothing cannot satisfy a reader that requests something, so
    // the pair would simply never match and the bridge would carry nothing.
    if (!profile_.empty()) {
        dds_qos::apply(writer_qos, profile_);
    }
    history_depth_ = writer_qos.history().depth;

    auto* topic_desc = participant_->lookup_topicdescription(spec_.address);
    topic_ = topic_desc ? dynamic_cast<efd::Topic*>(topic_desc)
                        : participant_->create_topic(spec_.address, type_support_.get_type_name(), topic_qos);
    if (!topic_) throw std::runtime_error("ExtDdsPub: failed to obtain topic '" + spec_.address + "'");

    listener_ = std::make_unique<WriterListener>(this);
    writer_ = publisher_->create_datawriter(topic_, writer_qos, listener_.get());
    if (!writer_) {
        listener_.reset();
        throw std::runtime_error("ExtDdsPub: create_datawriter failed for " + spec_.address);
    }

    msg_object_ = type_support_.get()->createData();
    started_ = true;
    spdlog::get("DDS_PUB")->debug("[{}] up on '{}' as {}", spec_.node_name, spec_.address, declared());
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
    listener_.reset();
    // Topic is left for the participant to clean up on destruction: another
    // endpoint in the same process may still hold it (lookup_topicdescription
    // above shares one Topic per name).
    topic_ = nullptr;
}

int64_t ExtDdsPub::send(uint64_t seq_key, size_t payload_bytes) {
    if (!started_) return 0;

    std::vector<uint8_t> payload(payload_bytes, static_cast<uint8_t>('A'));
    const int64_t t_pub_send = bench::prepend_header(payload, seq_key);
    if (stamp_) stamp_(payload, seq_key);

    const auto t0 = std::chrono::steady_clock::now();
    if (!DataConverter::payloadToMsg(message_type_, payload, msg_object_)) {
        throw std::runtime_error("ExtDdsPub: payloadToMsg failed for " + spec_.address);
    }
    const auto t1 = std::chrono::steady_clock::now();
    const int64_t conv_us = std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0).count();

    const size_t bench_size = payload.size();
    writer_->write(msg_object_);

    spdlog::get("DDS_PUB")->trace("[Send:{}] | Seq: {} | Conv: {} us | Size: {} B", spec_.address, seq_key, conv_us,
                                  bench_size);
    return t_pub_send;
}
