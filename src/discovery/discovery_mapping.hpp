#ifndef DISCOVERY_MAPPING_HPP
#define DISCOVERY_MAPPING_HPP

#include <string>
#include <vector>

#include "common/command.hpp"

/**
 * @struct DiscoveryMapping
 * @brief Mapping definition for discovery-based automatic command generation.
 *
 * Includes the command template passed to createBridge()/deleteBridge()
 * when discovery conditions are met.
 */
struct DiscoveryMapping {
    Command cmd_template;
};

/**
 * @brief Load discovery mappings from a JSON config file.
 *
 * Expected JSON schema:
 * {
 *   "mappings": [
 *     {
 *       "source": "DDS"|"SOMEIP"|"ZENOH",
 *       "sink":   "DDS"|"SOMEIP"|"ZENOH",
 *       "message_type": "STD_MSGS_MSG_STRING"|...,
 *       "qos": "RELIABLE"|"BEST_EFFORT",
 *       "bench_mode": false,                // optional, default false
 *       "topic": "...",                     // required when DDS appears as source or sink
 *       "zenoh_key": "...",                 // required when ZENOH appears as source or sink
 *       "service_id": 4660,                 // required when SOMEIP appears as source or sink
 *       "instance_id": 1,
 *       "eventgroup_id": 1,
 *       "event_id": 32769
 *     }
 *   ]
 * }
 *
 * @throws std::runtime_error on malformed JSON or missing required fields.
 */
std::vector<DiscoveryMapping> loadDiscoveryMappingsFromJson(const std::string& path);

#endif  // DISCOVERY_MAPPING_HPP
