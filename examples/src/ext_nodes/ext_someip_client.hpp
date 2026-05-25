#ifndef EXTERNAL_EXT_SOMEIP_CLIENT_HPP
#define EXTERNAL_EXT_SOMEIP_CLIENT_HPP

#include <vsomeip/vsomeip.hpp>

#include <cstdint>
#include <memory>

#include "ext_node.hpp"

class ExtSomeipClient : public ExtSubscriber {
public:
    ExtSomeipClient(std::shared_ptr<vsomeip::application> app, uint16_t service_id, uint16_t instance_id,
                    uint16_t eventgroup_id, uint16_t event_id);
    ~ExtSomeipClient() override;

    void start() override;
    void stop() override;

private:
    void onMessage(const std::shared_ptr<vsomeip::message>& msg);

    std::shared_ptr<vsomeip::application> app_;
    vsomeip::service_t s_id_;
    vsomeip::instance_t i_id_;
    vsomeip::eventgroup_t g_id_;
    vsomeip::event_t e_id_;
    bool started_ = false;
};

#endif  // EXTERNAL_EXT_SOMEIP_CLIENT_HPP
