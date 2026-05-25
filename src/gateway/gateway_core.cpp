#include "gateway_core.hpp"

#include <fastdds/dds/topic/TypeSupport.hpp>
#include <spdlog/sinks/stdout_color_sinks.h>

#include <cctype>
#include <sstream>

#include "ros2_msg_list.h"
#include "ros2_msg_type_registry.hpp"

GatewayCore::GatewayCore() : running_(false) {
    setup_global_logger();
    log_ = spdlog::get("CORE");
    if (!log_) {
        log_ = spdlog::stdout_color_mt("CORE");
        log_->set_level(spdlog::level::info);
    }

    // All runtime config paths come from env vars; main.cpp only constructs
    // GatewayCore and reports failure when construction throws.
    const char* mapping_env = std::getenv("GATEWAY_MAPPING_PATH");
    if (!mapping_env || !*mapping_env) {
        log_->critical("FATAL: GATEWAY_MAPPING_PATH env not set (path to mappings.json)");
        log_->critical("  Also required: ZENOH_CONFIG_PATH");
        log_->critical("  Also required (if mapping uses SOMEIP): VSOMEIP_CONFIG_PATH, VSOMEIP_APP_NAME");
        throw std::runtime_error("GATEWAY_MAPPING_PATH env not set");
    }
    const std::string config_path = mapping_env;
    discovery_mappings_ = loadDiscoveryMappingsFromJson(config_path);
    log_->info("Init: Loaded {} mapping(s) from {}", discovery_mappings_.size(), config_path);

    bool uses_someip = false;
    for (const auto& m : discovery_mappings_) {
        if (m.cmd_template.source == ProtocolType::SOMEIP || m.cmd_template.sink == ProtocolType::SOMEIP) {
            uses_someip = true;
            break;
        }
    }

    queue_map_ = std::make_shared<ConnectionQueueMap>();
    dds_tracker_ = std::make_shared<DDSResourceTracker>();
    zenoh_tracker_ = std::make_shared<ZenohResourceTracker>();
    someip_tracker_ = std::make_shared<SomeipResourceTracker>();

    try {
        eprosima::fastdds::dds::DomainParticipantQos pqos;
        pqos.name("Gateway");

        // Honor ROS_DOMAIN_ID so the gateway can be isolated from a co-located
        // ROS 2 stack (e.g. Exp4 server side uses domain 1 to force all
        // inter-host traffic through the Zenoh WAN bridge instead of LAN DDS).
        int domain_id = 0;
        if (const char* env_domain = std::getenv("ROS_DOMAIN_ID")) {
            try {
                domain_id = std::stoi(env_domain);
            } catch (...) {
                log_->warn("Invalid ROS_DOMAIN_ID='{}', falling back to 0", env_domain);
            }
        }
        log_->info("DDS DomainParticipant domain_id = {}", domain_id);

        dds_participant_ =
            eprosima::fastdds::dds::DomainParticipantFactory::get_instance()->create_participant(domain_id, pqos);
        if (!dds_participant_) {
            throw std::runtime_error("DomainParticipant creation failed");
        }
        DdsTypeRegistry::registerAllTypes(dds_participant_, log_);
    } catch (const std::exception& e) {
        log_->critical("FATAL: DDS DomainParticipant initialization failed: {}", e.what());
        throw;
    }

    if (uses_someip) {
        try {
            const char* vsomeip_config_env = std::getenv("VSOMEIP_CONFIG_PATH");
            const char* vsomeip_app_name_env = std::getenv("VSOMEIP_APP_NAME");
            if (!vsomeip_config_env || !vsomeip_app_name_env) {
                throw std::runtime_error(
                    "SOMEIP mapping present but VSOMEIP_CONFIG_PATH / VSOMEIP_APP_NAME env not set");
            }
            if (setenv("VSOMEIP_CONFIGURATION", vsomeip_config_env, 1) != 0) {
                log_->warn("  Failed to set VSOMEIP_CONFIGURATION env var");
            }
            if (setenv("VSOMEIP_APPLICATION_NAME", vsomeip_app_name_env, 1) != 0) {
                log_->warn("  Failed to set VSOMEIP_APPLICATION_NAME env var");
            }

            someip_app_ = vsomeip::runtime::get()->create_application(vsomeip_app_name_env);
            if (!someip_app_) {
                throw std::runtime_error("vsomeip::create_application returned nullptr");
            }
            if (!someip_app_->init()) {
                throw std::runtime_error("vsomeip::application::init() returned false");
            }
        } catch (const std::exception& e) {
            log_->critical("FATAL: vSomeIP initialization failed: {}", e.what());
            throw;
        }
    }

    try {
        const char* zenoh_config_env = std::getenv("ZENOH_CONFIG_PATH");
        if (!zenoh_config_env) {
            throw std::runtime_error("ZENOH_CONFIG_PATH env not set");
        }
        auto config = zenoh::Config::from_file(zenoh_config_env);
        zenoh_session_ = std::make_shared<zenoh::Session>(zenoh::Session::open(std::move(config)));
        if (!zenoh_session_) {
            throw std::runtime_error("zenoh::Session::open returned invalid session pointer");
        }
    } catch (const std::exception& e) {
        log_->critical("FATAL: Zenoh initialization failed: {}", e.what());
        throw;
    }

    try {
        dds_publisher_ = std::make_unique<DDSPublisher>(dds_participant_, dds_tracker_, queue_map_.get());
        dds_subscriber_ = std::make_unique<DDSSubscriber>(dds_participant_, dds_tracker_, queue_map_.get());
        if (uses_someip) {
            someip_server_ = std::make_unique<SomeipServer>(someip_app_, someip_tracker_, queue_map_.get());
            someip_client_ = std::make_unique<SomeipClient>(someip_app_, someip_tracker_, queue_map_.get());
        }
        zenoh_publisher_ = std::make_unique<ZenohPublisher>(zenoh_session_, zenoh_tracker_, queue_map_.get());
        zenoh_subscriber_ = std::make_unique<ZenohSubscriber>(zenoh_session_, zenoh_tracker_, queue_map_.get());
    } catch (const std::exception& e) {
        log_->critical("FATAL: Protocol handler initialization failed: {}", e.what());
        throw;
    }

    if (uses_someip) {
        someip_app_thread_ = std::thread([this]() { someip_app_->start(); });
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    discovery_manager_ = std::make_unique<DiscoveryManager>(dds_participant_, someip_app_, someip_tracker_,
                                                            zenoh_session_, this, discovery_mappings_);

    std::this_thread::sleep_for(std::chrono::seconds(1));
    running_ = true;
    log_->info("Init: Complete - System RUNNING");

    discovery_manager_->start();
    std::this_thread::sleep_for(std::chrono::seconds(1));
}

GatewayCore::~GatewayCore() { shutdown(); }

void GatewayCore::shutdown() {
    if (!running_.exchange(false)) return;
    log_->info("Shutdown: Started");

    // DiscoveryManager is only stopped here; its destruction is deferred to
    // the end of this function, after the DDS participant is gone. The
    // participant still holds a raw pointer to the manager's DiscoveryListener
    // until then, and any callback firing into freed memory would crash.
    if (discovery_manager_) {
        discovery_manager_->stop();
    }

    {
        // Drain any in-flight createBridge() / deleteBridge() before stopping
        // handlers underneath them.
        std::lock_guard<std::mutex> lock(mutex_);
    }

    if (dds_publisher_) dds_publisher_->stop();
    if (dds_subscriber_) dds_subscriber_->stop();
    if (someip_server_) someip_server_->stop();
    if (someip_client_) someip_client_->stop();
    if (zenoh_publisher_) zenoh_publisher_->stop();
    if (zenoh_subscriber_) zenoh_subscriber_->stop();

    dds_publisher_.reset();
    dds_subscriber_.reset();
    someip_server_.reset();
    someip_client_.reset();
    zenoh_publisher_.reset();
    zenoh_subscriber_.reset();

    if (someip_app_) {
        someip_app_->stop();
    }
    if (someip_app_thread_.joinable()) {
        someip_app_thread_.join();
    }

    if (dds_participant_) {
        dds_participant_->delete_contained_entities();
        eprosima::fastdds::dds::DomainParticipantFactory::get_instance()->delete_participant(dds_participant_);
        dds_participant_ = nullptr;
    }

    // Safe to destroy DiscoveryManager (and its listener) now — no more DDS
    // callbacks can fire into it.
    discovery_manager_.reset();

    zenoh_session_.reset();
    log_->info("Shutdown: Complete - System STOPPED");
}

bool GatewayCore::createBridge(Command& cmd) {
    std::lock_guard<std::mutex> lock(mutex_);

    logCommandDetails(cmd, "CREATE");

    if (!running_) {
        log_->warn("Creating: System not running - ignoring command");
        return false;
    }
    if (!validateCommand(cmd)) {
        log_->warn("Creating: Command validation failed - skipping");
        return false;
    }

    std::string key = make_conn_key(cmd);
    if (key.empty()) {
        log_->error("Creating: Connection key generation returned empty string");
        return false;
    }
    if (queue_map_->get(key) != nullptr) {
        log_->warn("Creating: Queue already exists for key '{}'", key);
        return false;
    }
    if (cmd.sink == ProtocolType::DDS && dds_tracker_->hasPubTopic(cmd.topic)) {
        log_->error("Creating: DDS publisher already exists for topic '{}'", cmd.topic);
        return false;
    }
    if (cmd.sink == ProtocolType::ZENOH && zenoh_tracker_->hasPubKey(cmd.zenoh_key)) {
        log_->error("Creating: Zenoh publisher already exists for key '{}'", cmd.zenoh_key);
        return false;
    }

    if (cmd.sink == ProtocolType::DDS) dds_publisher_->handleCreate(cmd);
    if (cmd.source == ProtocolType::DDS) dds_subscriber_->handleCreate(cmd);
    if (cmd.sink == ProtocolType::SOMEIP) someip_server_->handleCreate(cmd);
    if (cmd.source == ProtocolType::SOMEIP) someip_client_->handleCreate(cmd);
    if (cmd.sink == ProtocolType::ZENOH) zenoh_publisher_->handleCreate(cmd);
    if (cmd.source == ProtocolType::ZENOH) zenoh_subscriber_->handleCreate(cmd);
    return true;
}

bool GatewayCore::deleteBridge(Command& cmd) {
    std::lock_guard<std::mutex> lock(mutex_);

    logCommandDetails(cmd, "DELETE");

    if (!running_) {
        log_->warn("Deleting: System not running - ignoring command");
        return false;
    }
    if (!validateCommand(cmd)) {
        log_->warn("Deleting: Command validation failed - skipping");
        return false;
    }

    std::string key = make_conn_key(cmd);
    if (key.empty()) {
        log_->error("Deleting: Connection key generation returned empty string");
        return false;
    }
    if (queue_map_->get(key) == nullptr) {
        log_->warn("Deleting: Queue does not exist for key '{}'", key);
        return false;
    }

    if (cmd.sink == ProtocolType::DDS) dds_publisher_->handleDelete(cmd);
    if (cmd.source == ProtocolType::DDS) dds_subscriber_->handleDelete(cmd);
    if (cmd.sink == ProtocolType::SOMEIP) someip_server_->handleDelete(cmd);
    if (cmd.source == ProtocolType::SOMEIP) someip_client_->handleDelete(cmd);
    if (cmd.sink == ProtocolType::ZENOH) zenoh_publisher_->handleDelete(cmd);
    if (cmd.source == ProtocolType::ZENOH) zenoh_subscriber_->handleDelete(cmd);
    return true;
}

std::shared_ptr<ConnectionQueue> GatewayCore::getDataQueue(const std::string& key) {
    return queue_map_->get(key);
}

void GatewayCore::createDataQueue(const std::string& key) {
    queue_map_->create(key);
}

bool GatewayCore::validateCommand(Command& cmd) {
    int source_count = (cmd.source != ProtocolType::NONE) ? 1 : 0;
    int sink_count = (cmd.sink != ProtocolType::NONE) ? 1 : 0;
    int total_count = source_count + sink_count;

    if (total_count == 0) {
        log_->error("[Validate] FAILED: No source or sink specified");
        return false;
    }
    if (cmd.qos == Qos::NONE) {
        log_->error("[Validate] FAILED: QoS not specified (must be RELIABLE or BEST_EFFORT)");
        return false;
    }
    if (cmd.message_type == MessageType::NONE) {
        log_->error("[Validate] FAILED: MessageType not specified (must select a valid ROS2 message type)");
        return false;
    }

    bool is_dds_involved = (cmd.source == ProtocolType::DDS) || (cmd.sink == ProtocolType::DDS);
    if (!is_dds_involved && !cmd.topic.empty()) {
        log_->error("[Validate] FAILED: DDS not selected but 'topic' is set ({})", cmd.topic);
        return false;
    }

    if (is_dds_involved && !cmd.topic.empty()) {
        std::string original_topic = cmd.topic;

        // Auto-prefix "rt/" for ROS 2 compatibility.
        if (cmd.topic.front() == '/') {
            cmd.topic = "rt" + cmd.topic;
        } else if (cmd.topic.compare(0, 3, "rt/") != 0) {
            cmd.topic = "rt/" + cmd.topic;
        }
        if (original_topic != cmd.topic) {
            log_->debug("[ROS2] Topic auto-formatted: '{}' -> '{}'", original_topic, cmd.topic);
        }

        std::vector<std::string> error_reasons;
        std::string segment;
        std::istringstream iss(cmd.topic);

        for (char c : cmd.topic) {
            if (std::isupper(static_cast<unsigned char>(c))) {
                error_reasons.push_back("Topic must contain lowercase characters only");
                break;
            }
        }

        while (std::getline(iss, segment, '/')) {
            if (segment.empty()) {
                error_reasons.push_back("Empty segment detected (consecutive '/' or leading/trailing '/')");
                continue;
            }
            if (std::isdigit(static_cast<unsigned char>(segment[0]))) {
                error_reasons.push_back("Segment '" + segment + "' starts with a number");
            }
            if (segment.front() == '_' || segment.back() == '_') {
                error_reasons.push_back("Segment '" + segment + "' has leading/trailing underscore");
            }
            if (segment.find("__") != std::string::npos) {
                error_reasons.push_back("Segment '" + segment + "' contains consecutive underscores");
            }
            for (char c : segment) {
                if (!std::islower(static_cast<unsigned char>(c)) && !std::isdigit(static_cast<unsigned char>(c)) &&
                    c != '_') {
                    if (!std::isupper(static_cast<unsigned char>(c))) {
                        error_reasons.push_back("Segment '" + segment + "' contains invalid character: '" +
                                                std::string(1, c) + "'");
                    }
                }
            }
        }

        if (!error_reasons.empty()) {
            log_->error("[ROS2] Invalid topic naming convention: '{}'", cmd.topic);
            for (const auto& reason : error_reasons) {
                log_->error("  - {}", reason);
            }
            return false;
        }
    }

    bool is_zenoh_involved = (cmd.source == ProtocolType::ZENOH) || (cmd.sink == ProtocolType::ZENOH);
    if (!is_zenoh_involved && !cmd.zenoh_key.empty()) {
        log_->error("[Validate] FAILED: Zenoh not selected but 'zenoh_key' is set ({})", cmd.zenoh_key);
        return false;
    }

    bool is_someip_involved = (cmd.source == ProtocolType::SOMEIP) || (cmd.sink == ProtocolType::SOMEIP);
    if (!is_someip_involved && (cmd.service_id.has_value() || cmd.instance_id.has_value() ||
                                cmd.eventgroup_id.has_value() || cmd.event_id.has_value())) {
        log_->error("[Validate] FAILED: SOME/IP not selected but Service/Instance IDs are set");
        log_->error("  IDs: S:0x{:04x} I:0x{:04x}", cmd.service_id.value_or(0), cmd.instance_id.value_or(0));
        return false;
    }

    return true;
}

std::string GatewayCore::qosToString(Qos qos) {
    switch (qos) {
        case Qos::RELIABLE:    return "Reliable";
        case Qos::BEST_EFFORT: return "BestEffort";
        case Qos::NONE:        return "None";
    }
    return "Unknown";
}

static std::string protocolTypeToStr(ProtocolType p) {
    switch (p) {
        case ProtocolType::DDS:    return "DDS";
        case ProtocolType::SOMEIP: return "SOMEIP";
        case ProtocolType::ZENOH:  return "ZENOH";
        default:                   return "NONE";
    }
}

std::string GatewayCore::protocolRoutingToString(ProtocolType source, ProtocolType sink) {
    return protocolTypeToStr(source) + " -> " + protocolTypeToStr(sink);
}

void GatewayCore::logCommandDetails(const Command& cmd, const std::string& action) {
    std::string key = make_conn_key(cmd);

    const int BOX_WIDTH = 70;
    std::string h_line(BOX_WIDTH - 2, '-');
    std::string top_bar = "+" + h_line + "+";
    std::string bot_bar = "+" + h_line + "+";

    auto pad_line = [&](const std::string& label, const std::string& value) -> std::string {
        std::string content = "| " + label + ": " + value;
        int padding = BOX_WIDTH - static_cast<int>(content.length()) - 1;
        if (padding < 0) padding = 0;
        return content + std::string(padding, ' ') + "|";
    };

    std::stringstream ss;
    ss << "\n" << top_bar << "\n";
    ss << pad_line("Command", action) << "\n";
    ss << pad_line("Routing", protocolRoutingToString(cmd.source, cmd.sink)) << "\n";
    ss << pad_line("Message", DdsTypeRegistry::enumToString(cmd.message_type)) << "\n";
    ss << pad_line("QoS", qosToString(cmd.qos)) << "\n";
    ss << pad_line("Key", key) << "\n";

    if (!cmd.topic.empty()) {
        ss << pad_line("DDS Topic", cmd.topic) << "\n";
    }
    if (!cmd.zenoh_key.empty()) {
        ss << pad_line("Zenoh Key", cmd.zenoh_key) << "\n";
    }
    if (cmd.service_id.has_value()) {
        std::stringstream someip_ss;
        someip_ss << std::hex << "S:0x" << cmd.service_id.value_or(0) << " I:0x" << cmd.instance_id.value_or(0)
                  << " G:0x" << cmd.eventgroup_id.value_or(0) << " E:0x" << cmd.event_id.value_or(0);
        ss << pad_line("SOME/IP", someip_ss.str()) << "\n";
    }

    ss << bot_bar;

    log_->debug("[{}] Command received{}", action, ss.str());
}

void GatewayCore::logResourceStatus() {
    const int BOX_WIDTH = 80;
    std::string h_line(BOX_WIDTH - 2, '=');
    std::string thin_line(BOX_WIDTH - 2, '-');
    std::string top_bar = "+" + h_line + "+";
    std::string section_bar = "+" + thin_line + "+";
    std::string bot_bar = "+" + h_line + "+";

    auto pad_line = [&](const std::string& content) -> std::string {
        std::string line = "| " + content;
        int padding = BOX_WIDTH - static_cast<int>(line.length()) - 1;
        if (padding < 0) padding = 0;
        return line + std::string(padding, ' ') + "|";
    };

    auto center_line = [&](const std::string& content) -> std::string {
        int total_padding = BOX_WIDTH - static_cast<int>(content.length()) - 4;
        int left_pad = total_padding / 2;
        int right_pad = total_padding - left_pad;
        return "| " + std::string(left_pad, ' ') + content + std::string(right_pad, ' ') + " |";
    };

    std::stringstream ss;
    ss << "\n" << top_bar << "\n";
    ss << center_line("GATEWAY RESOURCE STATUS") << "\n";
    ss << top_bar << "\n";

    ss << center_line("[DDS] Topics") << "\n";
    ss << section_bar << "\n";
    {
        std::lock_guard<std::mutex> lock(dds_tracker_->mutex);
        std::set<std::string> all_dds_topics;
        for (const auto& topic : dds_tracker_->pub_topics) all_dds_topics.insert(topic);
        for (const auto& [topic, _] : dds_tracker_->sub_topic_refs) all_dds_topics.insert(topic);

        if (all_dds_topics.empty()) {
            ss << pad_line("  (none)") << "\n";
        } else {
            for (const auto& topic : all_dds_topics) {
                int pub_count = (dds_tracker_->pub_topics.find(topic) != dds_tracker_->pub_topics.end()) ? 1 : 0;
                int sub_count = 0;
                auto it = dds_tracker_->sub_topic_refs.find(topic);
                if (it != dds_tracker_->sub_topic_refs.end()) sub_count = it->second;
                ss << pad_line("  " + topic + " (pub:" + std::to_string(pub_count) +
                               ", sub:" + std::to_string(sub_count) + ")")
                   << "\n";
            }
        }
        ss << pad_line("  Total: " + std::to_string(all_dds_topics.size()) + " topic(s)") << "\n";
    }

    ss << section_bar << "\n";
    ss << center_line("[Zenoh] Keys") << "\n";
    ss << section_bar << "\n";
    {
        std::lock_guard<std::mutex> lock(zenoh_tracker_->mutex);
        std::set<std::string> all_zenoh_keys;
        for (const auto& key : zenoh_tracker_->pub_keys) all_zenoh_keys.insert(key);
        for (const auto& [key, _] : zenoh_tracker_->sub_key_refs) all_zenoh_keys.insert(key);

        if (all_zenoh_keys.empty()) {
            ss << pad_line("  (none)") << "\n";
        } else {
            for (const auto& key : all_zenoh_keys) {
                int pub_count = (zenoh_tracker_->pub_keys.find(key) != zenoh_tracker_->pub_keys.end()) ? 1 : 0;
                int sub_count = 0;
                auto it = zenoh_tracker_->sub_key_refs.find(key);
                if (it != zenoh_tracker_->sub_key_refs.end()) sub_count = it->second;
                ss << pad_line("  " + key + " (pub:" + std::to_string(pub_count) +
                               ", sub:" + std::to_string(sub_count) + ")")
                   << "\n";
            }
        }
        ss << pad_line("  Total: " + std::to_string(all_zenoh_keys.size()) + " key(s)") << "\n";
    }

    ss << section_bar << "\n";
    ss << center_line("[SOME/IP] Services") << "\n";
    ss << section_bar << "\n";
    {
        std::lock_guard<std::mutex> lock(someip_tracker_->mutex);

        if (someip_tracker_->services.empty()) {
            ss << pad_line("  (none)") << "\n";
        } else {
            for (const auto& [service_id, service_info] : someip_tracker_->services) {
                std::stringstream svc_ss;
                svc_ss << std::hex << std::uppercase;
                svc_ss << "  Service 0x" << service_id << " (pub:" << std::dec << service_info.server_count
                       << ", sub:" << service_info.client_count << ")";
                ss << pad_line(svc_ss.str()) << "\n";

                for (const auto& [instance_id, instance_info] : service_info.instances) {
                    std::stringstream inst_ss;
                    inst_ss << std::hex << std::uppercase;
                    inst_ss << "    Instance 0x" << instance_id << " (pub:" << std::dec << instance_info.server_count
                            << ", sub:" << instance_info.client_count << ")";
                    ss << pad_line(inst_ss.str()) << "\n";

                    for (const auto& [group_id, group_info] : instance_info.groups) {
                        std::stringstream grp_ss;
                        grp_ss << std::hex << std::uppercase;
                        grp_ss << "      EventGroup 0x" << group_id << " (pub:" << std::dec << group_info.server_count
                               << ", sub:" << group_info.client_count << ")";
                        ss << pad_line(grp_ss.str()) << "\n";

                        for (const auto& [event_id, event_info] : group_info.events) {
                            std::stringstream evt_ss;
                            evt_ss << std::hex << std::uppercase;
                            evt_ss << "        Event 0x" << event_id << " (pub:" << std::dec << event_info.server_count
                                   << ", sub:" << event_info.client_count << ")";
                            ss << pad_line(evt_ss.str()) << "\n";
                        }
                    }
                }
            }
        }
        ss << pad_line("  Total: " + std::to_string(someip_tracker_->services.size()) + " service(s)") << "\n";
    }

    ss << bot_bar;

    log_->info("{}", ss.str());
}
