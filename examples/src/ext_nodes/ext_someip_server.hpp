#ifndef EXTERNAL_EXT_SOMEIP_SERVER_HPP
#define EXTERNAL_EXT_SOMEIP_SERVER_HPP

#include <vsomeip/vsomeip.hpp>

#include <cstdint>
#include <memory>

#include "ext_node.hpp"

class ExtSomeipServer : public ExtPublisher {
public:
    ExtSomeipServer(std::shared_ptr<vsomeip::application> app, uint16_t service_id, uint16_t instance_id,
                    uint16_t eventgroup_id, uint16_t event_id);
    ~ExtSomeipServer() override;

    void start() override;
    void stop() override;
    void send(uint64_t seq_key, size_t payload_bytes) override;

private:
    std::shared_ptr<vsomeip::application> app_;
    vsomeip::service_t s_id_;
    vsomeip::instance_t i_id_;
    vsomeip::eventgroup_t g_id_;
    vsomeip::event_t e_id_;
    bool started_ = false;
};

#endif  // EXTERNAL_EXT_SOMEIP_SERVER_HPP
