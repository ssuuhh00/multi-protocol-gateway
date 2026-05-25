#include "common/logger_config.hpp"

#include <spdlog/async.h>
#include <spdlog/sinks/basic_file_sink.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/spdlog.h>

#include <cctype>
#include <cstdlib>
#include <memory>
#include <string>
#include <vector>

void setup_global_logger() {
    static std::once_flag init_flag;

    std::call_once(init_flag, []() {
        constexpr bool USE_ASYNC_LOGGING = true;

        if (USE_ASYNC_LOGGING) {
            spdlog::init_thread_pool(8192, 1);
        }

        const char* log_env = std::getenv("LOG_PATH");
        std::string log_path = (log_env != nullptr) ? log_env : DEFAULT_LOG_PATH;

        spdlog::level::level_enum default_level = spdlog::level::info;
        if (const char* lvl_env = std::getenv("LOG_LEVEL")) {
            std::string s = lvl_env;
            for (auto& c : s) c = static_cast<char>(::tolower(c));
            if (s == "trace") default_level = spdlog::level::trace;
            else if (s == "debug") default_level = spdlog::level::debug;
            else if (s == "info") default_level = spdlog::level::info;
            else if (s == "warn" || s == "warning") default_level = spdlog::level::warn;
            else if (s == "error") default_level = spdlog::level::err;
        }

        std::vector<spdlog::sink_ptr> sinks;

        // Console sink is opt-out via LOG_CONSOLE=0 (useful for batch sweeps
        // where every terminal line is wasted I/O). File sink is always on.
        const char* console_env = std::getenv("LOG_CONSOLE");
        const bool console_enabled = !(console_env && std::string(console_env) == "0");
        if (console_enabled) {
            auto console_sink = std::make_shared<spdlog::sinks::stdout_color_sink_mt>();
            console_sink->set_pattern("[%^%n%$] %v");
            sinks.push_back(console_sink);
        }

        auto file_sink = std::make_shared<spdlog::sinks::basic_file_sink_mt>(log_path, true);
        file_sink->set_pattern("[%Y-%m-%d %H:%M:%S.%e] [%n] [%l] %v");
        sinks.push_back(file_sink);

        // LOG_LEVEL env wins over the per-logger level so verbosity stays
        // consistent across every logger registered in one process.
        auto register_logger = [&](std::string name, spdlog::level::level_enum level = spdlog::level::info) {
            if (std::getenv("LOG_LEVEL")) level = default_level;
            std::shared_ptr<spdlog::logger> logger;
            if (USE_ASYNC_LOGGING) {
                logger = std::make_shared<spdlog::async_logger>(name, sinks.begin(), sinks.end(), spdlog::thread_pool(),
                                                                spdlog::async_overflow_policy::block);
            } else {
                logger = std::make_shared<spdlog::logger>(name, sinks.begin(), sinks.end());
            }
            logger->set_level(level);
            logger->flush_on(spdlog::level::err);
            spdlog::register_logger(logger);
        };

        // DEMO / EXT exist so non-gateway binaries are visually distinguishable
        // from the gateway when their stdout is mixed in the same terminal.
        register_logger("MAIN");
        register_logger("DEMO");
        register_logger("EXT");
        register_logger("CORE");
        register_logger("DDS_PUB");
        register_logger("DDS_SUB");
        register_logger("SMP_SRV");
        register_logger("SMP_CLI");
        register_logger("ZEN_PUB");
        register_logger("ZEN_SUB");
        register_logger("DISCOVERY");

        spdlog::flush_every(std::chrono::seconds(3));
    });
}