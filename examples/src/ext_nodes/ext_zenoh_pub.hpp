#ifndef EXTERNAL_EXT_ZENOH_PUB_HPP
#define EXTERNAL_EXT_ZENOH_PUB_HPP

#include <memory>
#include <optional>
#include <string>

#include <zenoh.hxx>

#include "ext_node.hpp"

class ExtZenohPub : public ExtPublisher {
public:
    ExtZenohPub(std::shared_ptr<zenoh::Session> session, std::string key);
    ~ExtZenohPub() override;

    void start() override;
    void stop() override;
    void send(uint64_t seq_key, size_t payload_bytes) override;

private:
    std::shared_ptr<zenoh::Session> session_;
    std::string key_;
    std::optional<zenoh::Publisher> publisher_;
    std::optional<zenoh::LivelinessToken> liveliness_token_;
    bool started_ = false;
};

#endif  // EXTERNAL_EXT_ZENOH_PUB_HPP
