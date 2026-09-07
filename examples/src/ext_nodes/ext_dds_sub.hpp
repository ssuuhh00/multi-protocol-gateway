#ifndef EXTERNAL_EXT_DDS_SUB_HPP
#define EXTERNAL_EXT_DDS_SUB_HPP

#include <fastdds/dds/domain/DomainParticipant.hpp>
#include <fastdds/dds/subscriber/DataReader.hpp>
#include <fastdds/dds/subscriber/DataReaderListener.hpp>
#include <fastdds/dds/subscriber/Subscriber.hpp>
#include <fastdds/dds/topic/Topic.hpp>
#include <fastdds/dds/topic/TypeSupport.hpp>

#include <atomic>
#include <memory>
#include <string>

#include "common/command.hpp"
#include "ext_node.hpp"

// DDS consumer: reads the topic the gateway's DDS sink leg writes.
class ExtDdsSub : public ExtSubscriber {
public:
    // `profile` is the mapping's qos_policies block. A QoS contract holds
    // between two peers, so an endpoint that does not declare what the gateway
    // declares is not a counterpart -- it is a different experiment, and on
    // the DDS request/offer rules it may not match at all. This reader is the
    // endpoint the gateway's offered DEADLINE is for, so it applies the full
    // profile, deadline included.
    ExtDdsSub(eprosima::fastdds::dds::DomainParticipant* participant, EndpointSpec spec, MessageType mt,
           Qos qos, const QosProfile& profile = QosProfile{});
    ~ExtDdsSub() override;

    void start() override;
    void stop() override;

    const char* protocol() const override { return "DDS"; }
    std::string declared() const override;

    uint32_t matched_writers() const { return matched_.load(std::memory_order_relaxed); }
    // Samples Fast DDS reported as lost or rejected before this reader saw them.
    // Without these two, a message the demo never receives is indistinguishable
    // from one the gateway never sent.
    uint64_t lost_samples() const { return lost_.load(std::memory_order_relaxed); }
    // Non-zero means this reader's requested QoS was incompatible with an
    // offered writer, so the two never matched at all.
    uint64_t incompatible_qos() const { return incompatible_.load(std::memory_order_relaxed); }
    // Missed periods this reader's OWN middleware reported. Non-zero only when
    // the mapping declared a deadline and the writer went silent for longer
    // than it: the consumer-side half of the DEADLINE row (qos_spec.cpp), the
    // half no gateway log can stand in for.
    uint64_t deadline_missed() const { return deadline_missed_.load(std::memory_order_relaxed); }

private:
    class ReaderListener : public eprosima::fastdds::dds::DataReaderListener {
    public:
        ReaderListener(ExtDdsSub* owner, eprosima::fastdds::dds::TypeSupport ts, MessageType mt);

        void on_data_available(eprosima::fastdds::dds::DataReader* reader) override;
        void on_subscription_matched(eprosima::fastdds::dds::DataReader* reader,
                                     const eprosima::fastdds::dds::SubscriptionMatchedStatus& info) override;
        void on_sample_lost(eprosima::fastdds::dds::DataReader* reader,
                            const eprosima::fastdds::dds::SampleLostStatus& status) override;
        void on_requested_incompatible_qos(eprosima::fastdds::dds::DataReader* reader,
                                           const eprosima::fastdds::dds::RequestedIncompatibleQosStatus& status) override;
        void on_requested_deadline_missed(eprosima::fastdds::dds::DataReader* reader,
                                          const eprosima::fastdds::dds::RequestedDeadlineMissedStatus& status) override;

    private:
        ExtDdsSub* owner_;
        eprosima::fastdds::dds::TypeSupport type_support_;
        MessageType message_type_;
    };

    eprosima::fastdds::dds::DomainParticipant* participant_ = nullptr;
    MessageType message_type_;
    Qos qos_;
    QosProfile profile_;

    eprosima::fastdds::dds::TypeSupport type_support_;
    eprosima::fastdds::dds::Subscriber* subscriber_ = nullptr;
    eprosima::fastdds::dds::Topic* topic_ = nullptr;
    eprosima::fastdds::dds::DataReader* reader_ = nullptr;
    std::unique_ptr<ReaderListener> listener_;

    std::atomic<uint32_t> matched_{0};
    std::atomic<uint64_t> lost_{0};
    std::atomic<uint64_t> incompatible_{0};
    std::atomic<uint64_t> deadline_missed_{0};
    int32_t history_depth_ = 0;
};

#endif  // EXTERNAL_EXT_DDS_SUB_HPP
