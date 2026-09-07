#ifndef EXTERNAL_EXT_SOMEIP_CLIENT_HPP
#define EXTERNAL_EXT_SOMEIP_CLIENT_HPP

#include <vsomeip/vsomeip.hpp>

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>

#include "common/qos_profile.hpp"
#include "ext_node.hpp"

// SOME/IP consumer: subscribes to the service+event the gateway's SOME/IP sink
// leg (a server) offers.
//
// As in ExtSomeipServer, the profile is evaluated at the GATEWAY's leg role
// -- SINK here -- because the registration this one has to agree with is the
// gateway's, not its own.
class ExtSomeipClient : public ExtSubscriber {
public:
    ExtSomeipClient(std::shared_ptr<vsomeip::application> app, EndpointSpec spec, uint16_t service_id,
                    uint16_t instance_id, uint16_t eventgroup_id, uint16_t event_id,
                    const QosProfile& profile = QosProfile{});
    ~ExtSomeipClient() override;

    void start() override;
    void stop() override;

    const char* protocol() const override { return "SOMEIP"; }
    std::string declared() const override;

    // Service Discovery says the offered service is reachable. Distinguishes
    // "the gateway never offered" from "it offered but sends nothing".
    bool service_available() const { return available_.load(std::memory_order_relaxed); }

private:
    void onMessage(const std::shared_ptr<vsomeip::message>& msg);

    std::shared_ptr<vsomeip::application> app_;
    vsomeip::service_t s_id_;
    vsomeip::instance_t i_id_;
    vsomeip::eventgroup_t g_id_;
    vsomeip::event_t e_id_;
    QosProfile profile_;
    std::atomic<bool> available_{false};
};

#endif  // EXTERNAL_EXT_SOMEIP_CLIENT_HPP
