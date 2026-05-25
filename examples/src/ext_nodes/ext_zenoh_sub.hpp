#ifndef EXTERNAL_EXT_ZENOH_SUB_HPP
#define EXTERNAL_EXT_ZENOH_SUB_HPP

#include <memory>
#include <optional>
#include <string>

#include <zenoh.hxx>

#include "ext_node.hpp"

class ExtZenohSub : public ExtSubscriber {
public:
    ExtZenohSub(std::shared_ptr<zenoh::Session> session, std::string key);
    ~ExtZenohSub() override;

    void start() override;
    void stop() override;

private:
    std::shared_ptr<zenoh::Session> session_;
    std::string key_;
    std::optional<zenoh::Subscriber<void>> subscriber_;
    std::optional<zenoh::LivelinessToken> liveliness_token_;
    bool started_ = false;
};

#endif  // EXTERNAL_EXT_ZENOH_SUB_HPP
