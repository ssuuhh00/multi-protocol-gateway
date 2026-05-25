#ifndef COMMON_LOGGER_CONFIG_HPP
#define COMMON_LOGGER_CONFIG_HPP

// Initialize the global spdlog loggers (MAIN/CORE/DDS_PUB/DDS_SUB/SMP_SRV/
// SMP_CLI/ZEN_PUB/ZEN_SUB/DISCOVERY/DEMO/EXT). Idempotent via std::call_once.
void setup_global_logger();

#endif  // COMMON_LOGGER_CONFIG_HPP
