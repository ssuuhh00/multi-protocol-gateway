#ifndef EXTERNAL_EXT_SOMEIP_SERVER_HPP
#define EXTERNAL_EXT_SOMEIP_SERVER_HPP

#include <vsomeip/vsomeip.hpp>

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>

#include "common/qos_profile.hpp"
#include "ext_node.hpp"

// SOME/IP producer: offers the service+event the gateway's SOME/IP source leg
// (a client) subscribes to, and notifies on it.
//
// The event type comes from the same mapping the gateway reads, through the
// same function. vsomeip keeps one event type per (service, instance, event)
// and rejects a second registration that disagrees, so the question is never
// "what would this endpoint like" but "what did the peer register" -- and
// someip_qos::eventType() answers it from durability alone, the same on both
// roles, so this endpoint and the gateway's SOURCE leg cannot disagree.
class ExtSomeipServer : public ExtPublisher {
public:
    ExtSomeipServer(std::shared_ptr<vsomeip::application> app, EndpointSpec spec, uint16_t service_id,
                    uint16_t instance_id, uint16_t eventgroup_id, uint16_t event_id,
                    const QosProfile& profile = QosProfile{});
    ~ExtSomeipServer() override;

    void start() override;
    void stop() override;
    int64_t send(uint64_t seq_key, size_t payload_bytes) override;

    const char* protocol() const override { return "SOMEIP"; }
    std::string declared() const override;

    // Whether any client has subscribed to our eventgroup. notify() on an
    // unsubscribed event is silently dropped by vsomeip, so without this a
    // producer that nobody is listening to looks identical to a working one.
    bool has_subscriber() const { return subscribed_.load(std::memory_order_relaxed); }

private:
    std::shared_ptr<vsomeip::application> app_;
    vsomeip::service_t s_id_;
    vsomeip::instance_t i_id_;
    vsomeip::eventgroup_t g_id_;
    vsomeip::event_t e_id_;
    QosProfile profile_;
    std::atomic<bool> subscribed_{false};
};

#endif  // EXTERNAL_EXT_SOMEIP_SERVER_HPP
