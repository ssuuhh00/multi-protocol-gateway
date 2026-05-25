#include <spdlog/spdlog.h>

#include <chrono>
#include <csignal>
#include <cstdlib>
#include <exception>
#include <memory>
#include <thread>

#include "common/logger_config.hpp"
#include "gateway/gateway_core.hpp"

namespace {

volatile sig_atomic_t g_shutdown_requested = 0;

void signal_handler(int signum) {
    if (g_shutdown_requested) {
        _exit(128 + signum);
    }
    g_shutdown_requested = 1;
}

}  // namespace

int main(int /*argc*/, char* /*argv*/[]) {
    std::signal(SIGINT, signal_handler);
    std::signal(SIGTERM, signal_handler);

    setup_global_logger();
    auto log_ = spdlog::get("MAIN");

    log_->info(">>> Starting Gateway");

    std::unique_ptr<GatewayCore> gateway;
    try {
        gateway = std::make_unique<GatewayCore>();
    } catch (const std::exception& e) {
        log_->error(">>> Gateway creation failed: {}", e.what());
        return -1;
    }

    log_->info(">>> Gateway ready. Waiting for discovery events. Send SIGINT/SIGTERM to stop.");

    while (!g_shutdown_requested) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    log_->info(">>> Shutdown requested.");

    log_->info(">>> Gateway exited cleanly.");
    return 0;
}
