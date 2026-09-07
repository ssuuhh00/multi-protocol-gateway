#ifndef EXTERNAL_EXT_ZENOH_PUB_HPP
#define EXTERNAL_EXT_ZENOH_PUB_HPP

#include <memory>
#include <optional>
#include <string>

#include <zenoh.hxx>

#include "common/qos_profile.hpp"
#include "ext_node.hpp"

// Zenoh producer: publishes on the key the gateway's Zenoh source leg subscribes to.
//
// Takes the mapping's qos_policies for the same reason the DDS endpoints do: a
// declaration is a statement about the bridge, and an endpoint that ignores it
// is not the counterpart the gateway is being tested against. Zenoh has no
// request/offer rules, so an undeclared endpoint here does not break matching
// the way it does on DDS -- it just makes the monitor's qos column read
// "default" while the gateway prints four carried policies, which is the
// discrepancy that prompted this.
class ExtZenohPub : public ExtPublisher {
public:
    ExtZenohPub(std::shared_ptr<zenoh::Session> session, EndpointSpec spec, const QosProfile& profile = QosProfile{});
    ~ExtZenohPub() override;

    void start() override;
    void stop() override;
    int64_t send(uint64_t seq_key, size_t payload_bytes) override;

    const char* protocol() const override { return "ZENOH"; }
    std::string declared() const override;

private:
    std::shared_ptr<zenoh::Session> session_;
    QosProfile profile_;
    std::optional<zenoh::Publisher> publisher_;
    std::optional<zenoh::ext::AdvancedPublisher> advanced_;

    // The discovery token this node has always declared, under "@mpg", so the
    // gateway's DiscoveryManager can see it.
    std::optional<zenoh::LivelinessToken> liveliness_token_;

    // The LIVELINESS QoS assertion on the data key expression, and the reason
    // this class takes a profile at all. ext_node.hpp names liveliness as the
    // clearest case of a policy that proves nothing when only one side declares
    // it: a token nobody on the other side watches proves nothing.
    std::optional<zenoh::LivelinessToken> liveliness_;

    bool started_ = false;
};

#endif  // EXTERNAL_EXT_ZENOH_PUB_HPP
