#include "ext_dds_sub.hpp"

#include <fastdds/dds/subscriber/SampleInfo.hpp>
#include <fastdds/dds/subscriber/qos/DataReaderQos.hpp>
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

ExtDdsSub::ReaderListener::ReaderListener(ExtDdsSub* owner, efd::TypeSupport ts, MessageType mt)
    : owner_(owner), type_support_(ts), message_type_(mt) {}

void ExtDdsSub::ReaderListener::on_subscription_matched(efd::DataReader* /*reader*/,
                                                        const efd::SubscriptionMatchedStatus& info) {
    owner_->matched_.store(static_cast<uint32_t>(info.current_count), std::memory_order_relaxed);
    spdlog::get("DDS_SUB")->debug("[{}] subscription matched: {:+d} -> {} writer(s)", owner_->spec_.address,
                                  info.current_count_change, info.current_count);
}

void ExtDdsSub::ReaderListener::on_sample_lost(efd::DataReader* /*reader*/, const efd::SampleLostStatus& status) {
    owner_->lost_.store(static_cast<uint64_t>(status.total_count), std::memory_order_relaxed);
    spdlog::get("DDS_SUB")->warn("[{}] sample lost: total={}", owner_->spec_.address, status.total_count);
}

void ExtDdsSub::ReaderListener::on_requested_incompatible_qos(efd::DataReader* /*reader*/,
                                                              const efd::RequestedIncompatibleQosStatus& status) {
    owner_->incompatible_.store(static_cast<uint64_t>(status.total_count), std::memory_order_relaxed);
    // last_policy_id names the single policy that blocked the match. Reporting
    // it is the difference between "nothing arrives" and "nothing arrives
    // because reliability was requested where best-effort was offered".
    spdlog::get("DDS_SUB")->error("[{}] REQUESTED_INCOMPATIBLE_QOS: total={} last_policy_id={}", owner_->spec_.address,
                                  status.total_count, static_cast<int>(status.last_policy_id));
}

void ExtDdsSub::ReaderListener::on_requested_deadline_missed(efd::DataReader* /*reader*/,
                                                            const efd::RequestedDeadlineMissedStatus& status) {
    owner_->deadline_missed_.store(static_cast<uint64_t>(status.total_count), std::memory_order_relaxed);
    // This reader asked for a period and Fast DDS says the writer did not keep
    // it. It is the evidence the DEADLINE row (z2d, s2d) rests on: a missed
    // period reported to the CONSUMER by the consumer's own middleware, not by
    // the gateway. Fast DDS fires once per missed period for as long as the
    // silence lasts, so the first is at warn and the rest at debug.
    auto log = spdlog::get("DDS_SUB");
    if (status.total_count <= 1)
        log->warn("[{}] REQUESTED_DEADLINE_MISSED: total={}", owner_->spec_.address, status.total_count);
    else
        log->debug("[{}] REQUESTED_DEADLINE_MISSED: total={}", owner_->spec_.address, status.total_count);
}

void ExtDdsSub::ReaderListener::on_data_available(efd::DataReader* reader) {
    void* data = type_support_.get()->createData();
    efd::SampleInfo info;
    if (reader->take_next_sample(data, &info) == eprosima::fastrtps::types::ReturnCode_t::RETCODE_OK) {
        if (info.valid_data) {
            const auto t_a = std::chrono::steady_clock::now();
            std::vector<uint8_t> payload = DataConverter::msgToPayload(message_type_, data);
            const auto t_ready = std::chrono::steady_clock::now();
            const int64_t conv_us = std::chrono::duration_cast<std::chrono::microseconds>(t_ready - t_a).count();

            if (!payload.empty()) {
                const uint64_t seq_key = bench::read_slot(payload, bench::Slot::SEQ_KEY);
                const int64_t t_leave = static_cast<int64_t>(bench::read_slot(payload, bench::Slot::TS));
                const int64_t sink_mw_us = (t_ready.time_since_epoch().count() - t_leave) / 1000;
                spdlog::get("DDS_SUB")->trace("[Callback:{}] | Seq: {} | SinkMW: {} us | Conv: {} us | Size: {} B",
                                              owner_->spec_.address, seq_key, sink_mw_us, conv_us, payload.size());

                if (owner_->on_message_) {
                    owner_->on_message_(payload, t_ready.time_since_epoch().count());
                }
            }
        }
    }
    type_support_.get()->deleteData(data);
}

ExtDdsSub::ExtDdsSub(efd::DomainParticipant* participant, EndpointSpec spec, MessageType mt, Qos qos,
                     const QosProfile& profile)
    : ExtSubscriber(std::move(spec)), participant_(participant), message_type_(mt), qos_(qos), profile_(profile) {
    if (!participant_) throw std::runtime_error("ExtDdsSub: null participant");
}

ExtDdsSub::~ExtDdsSub() { stop(); }

std::string ExtDdsSub::declared() const {
    std::string s = (qos_ == Qos::BEST_EFFORT) ? "BEST_EFFORT" : "RELIABLE";
    s += " KEEP_LAST(" + std::to_string(history_depth_) + ")";
    // The period this reader REQUESTED. Printed so the settle-time declaration
    // line shows both ends asked for the same deadline (the positive control).
    if (profile_.deadline_ms) s += " deadline=" + std::to_string(*profile_.deadline_ms) + "ms";
    return s;
}

void ExtDdsSub::start() {
    if (started_) return;

    type_support_ = DdsTypeRegistry::createTypeSupport(message_type_);
    if (!type_support_) {
        throw std::runtime_error("ExtDdsSub: TypeSupport creation failed for " + spec_.address);
    }

    subscriber_ = participant_->create_subscriber(efd::SUBSCRIBER_QOS_DEFAULT, nullptr);
    if (!subscriber_) throw std::runtime_error("ExtDdsSub: create_subscriber failed for " + spec_.address);

    efd::TopicQos topic_qos = efd::TOPIC_QOS_DEFAULT;
    efd::DataReaderQos reader_qos = efd::DATAREADER_QOS_DEFAULT;

    const auto kind = (qos_ == Qos::BEST_EFFORT) ? efd::BEST_EFFORT_RELIABILITY_QOS : efd::RELIABLE_RELIABILITY_QOS;
    topic_qos.reliability().kind = kind;
    reader_qos.reliability().kind = kind;
    reader_qos.history().depth = 10;
    // See the note in ext_dds_pub.cpp: the declaration has to be on both ends
    // or it is not a contract.
    if (!profile_.empty()) {
        dds_qos::apply(reader_qos, profile_);
    }
    history_depth_ = reader_qos.history().depth;

    auto* topic_desc = participant_->lookup_topicdescription(spec_.address);
    topic_ = topic_desc ? dynamic_cast<efd::Topic*>(topic_desc)
                        : participant_->create_topic(spec_.address, type_support_.get_type_name(), topic_qos);
    if (!topic_) throw std::runtime_error("ExtDdsSub: failed to obtain topic '" + spec_.address + "'");

    listener_ = std::make_unique<ReaderListener>(this, type_support_, message_type_);
    reader_ = subscriber_->create_datareader(topic_, reader_qos, listener_.get());
    if (!reader_) {
        listener_.reset();
        throw std::runtime_error("ExtDdsSub: create_datareader failed for " + spec_.address);
    }
    started_ = true;
    spdlog::get("DDS_SUB")->debug("[{}] up on '{}' as {}", spec_.node_name, spec_.address, declared());
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
    listener_.reset();
    topic_ = nullptr;
}
