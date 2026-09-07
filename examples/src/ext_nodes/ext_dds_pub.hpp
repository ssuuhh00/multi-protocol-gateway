#ifndef EXTERNAL_EXT_DDS_PUB_HPP
#define EXTERNAL_EXT_DDS_PUB_HPP

#include <fastdds/dds/domain/DomainParticipant.hpp>
#include <fastdds/dds/publisher/DataWriter.hpp>
#include <fastdds/dds/publisher/DataWriterListener.hpp>
#include <fastdds/dds/publisher/Publisher.hpp>
#include <fastdds/dds/topic/Topic.hpp>
#include <fastdds/dds/topic/TypeSupport.hpp>

#include <atomic>
#include <memory>
#include <string>

#include "common/command.hpp"
#include "ext_node.hpp"

// DDS producer: writes into the topic the gateway's DDS source leg reads.
class ExtDdsPub : public ExtPublisher {
public:
    // `profile` is the mapping's qos_policies block. A QoS contract holds
    // between two peers, so an endpoint that does not declare what the gateway
    // declares is not a counterpart -- it is a different experiment, and on
    // the DDS request/offer rules it may not match at all.
    ExtDdsPub(eprosima::fastdds::dds::DomainParticipant* participant, EndpointSpec spec, MessageType mt,
           Qos qos, const QosProfile& profile = QosProfile{});
    ~ExtDdsPub() override;

    void start() override;
    void stop() override;
    int64_t send(uint64_t seq_key, size_t payload_bytes) override;

    const char* protocol() const override { return "DDS"; }
    std::string declared() const override;

    // Peers currently matched with this writer. The gateway's own DDS subscriber
    // is one of them, so 0 here means the bridge cannot possibly be carrying
    // traffic -- a distinct failure from "the bridge is up but idle".
    uint32_t matched_readers() const { return matched_.load(std::memory_order_relaxed); }

private:
    // Counts discovery matches. A writer with no listener cannot tell whether
    // anyone is listening, which is exactly the blind spot that makes an
    // unreachable demo look identical to an idle one.
    class WriterListener : public eprosima::fastdds::dds::DataWriterListener {
    public:
        explicit WriterListener(ExtDdsPub* owner) : owner_(owner) {}
        void on_publication_matched(eprosima::fastdds::dds::DataWriter* writer,
                                    const eprosima::fastdds::dds::PublicationMatchedStatus& info) override;

    private:
        ExtDdsPub* owner_;
    };

    eprosima::fastdds::dds::DomainParticipant* participant_ = nullptr;
    MessageType message_type_;
    Qos qos_;
    QosProfile profile_;

    eprosima::fastdds::dds::TypeSupport type_support_;
    eprosima::fastdds::dds::Publisher* publisher_ = nullptr;
    eprosima::fastdds::dds::Topic* topic_ = nullptr;
    eprosima::fastdds::dds::DataWriter* writer_ = nullptr;
    std::unique_ptr<WriterListener> listener_;
    void* msg_object_ = nullptr;

    std::atomic<uint32_t> matched_{0};
    // History depth actually handed to Fast DDS. Kept so declared() reports the
    // value that was set, not the value the config wished for.
    int32_t history_depth_ = 0;
};

#endif  // EXTERNAL_EXT_DDS_PUB_HPP
