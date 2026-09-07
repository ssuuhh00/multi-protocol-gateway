#ifndef EXTERNAL_EXT_ZENOH_SUB_HPP
#define EXTERNAL_EXT_ZENOH_SUB_HPP

#include <memory>
#include <optional>
#include <string>

#include <zenoh.hxx>

#include "common/qos_profile.hpp"
#include "ext_node.hpp"

// Zenoh consumer: subscribes to the key the gateway's Zenoh sink leg publishes on.
//
// The profile matters most here, and for one policy in particular. A late
// joiner receives past samples only if it is an AdvancedSubscriber whose
// `history` option is set -- the publisher's cache does not push to newcomers,
// the subscriber queries it. So a plain subscriber makes the gateway's declared
// TRANSIENT_LOCAL unobservable from outside, which is what a late joiner has to
// see.
class ExtZenohSub : public ExtSubscriber {
public:
    ExtZenohSub(std::shared_ptr<zenoh::Session> session, EndpointSpec spec, const QosProfile& profile = QosProfile{});
    ~ExtZenohSub() override;

    void start() override;
    void stop() override;

    const char* protocol() const override { return "ZENOH"; }
    std::string declared() const override;

private:
    std::shared_ptr<zenoh::Session> session_;
    QosProfile profile_;
    std::optional<zenoh::Subscriber<void>> subscriber_;
    // The discovery token this node declares under "@mpg" so the gateway can
    // see it.
    std::optional<zenoh::LivelinessToken> liveliness_token_;

    // Watches the gateway's LIVELINESS QoS token on this key. Separate from
    // subscriber_ because it lives in Zenoh's liveliness keyspace, not the data
    // one, and separate from liveliness_token_ because that one is ours.
    std::optional<zenoh::Subscriber<void>> liveliness_;
    std::optional<zenoh::ext::AdvancedSubscriber<void>> advanced_;

    bool started_ = false;
};

#endif  // EXTERNAL_EXT_ZENOH_SUB_HPP
