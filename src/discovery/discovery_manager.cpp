#include "discovery/discovery_manager.hpp"

#include <spdlog/sinks/stdout_color_sinks.h>

#include <cctype>
#include <set>
#include <sstream>
#include <utility>

#include "common/zenoh_liveliness.hpp"
#include "gateway/gateway_core.hpp"

DiscoveryManager::DiscoveryManager(eprosima::fastdds::dds::DomainParticipant* dds_participant,
                                   std::shared_ptr<vsomeip::application> someip_app,
                                   std::shared_ptr<SomeipResourceTracker> someip_tracker,
                                   std::shared_ptr<zenoh::Session> zenoh_session, GatewayCore* core,
                                   const std::vector<DiscoveryMapping>& mappings)
    : dds_participant_(dds_participant),
      someip_app_(someip_app),
      someip_tracker_(someip_tracker),
      zenoh_session_(std::move(zenoh_session)),
      core_(core) {
    log_ = spdlog::get("DISCOVERY");
    if (!log_) {
        log_ = spdlog::stdout_color_mt("DISCOVERY");
        log_->set_level(spdlog::level::info);
    }

    if (dds_participant_) {
        self_guid_prefix_ = dds_participant_->guid().guidPrefix;
    }
    if (zenoh_session_) {
        self_zid_hex_ = zenoh_session_->get_zid().to_string();
        log_->debug("Init: Self Zenoh ZID = {}", self_zid_hex_);
    }

    for (size_t idx = 0; idx < mappings.size(); ++idx) {
        auto m = mappings[idx];

        if (!validateMapping(m.cmd_template, idx)) {
            continue;
        }

        MappingState state;
        state.mapping = m;

        // source_condition false means "waiting for external endpoint":
        //   DDS src    -> external DDS Writer
        //   SOMEIP src -> external SOME/IP Server availability
        //   ZENOH src  -> external Zenoh publisher liveliness
        switch (m.cmd_template.source) {
            case ProtocolType::DDS:
            case ProtocolType::SOMEIP:
            case ProtocolType::ZENOH:
                state.source_condition = false;
                break;
            default:
                state.source_condition = true;
                break;
        }

        // sink_condition false means "waiting for external endpoint":
        //   DDS sink    -> external DDS Reader
        //   SOMEIP sink -> external SOME/IP Client subscription
        //   ZENOH sink  -> external Zenoh subscriber liveliness
        switch (m.cmd_template.sink) {
            case ProtocolType::DDS:
            case ProtocolType::SOMEIP:
            case ProtocolType::ZENOH:
                state.sink_condition = false;
                break;
            default:
                state.sink_condition = true;
                break;
        }

        states_.push_back(std::move(state));
    }
    log_->debug("Init: Loaded {} mapping(s) ({} valid)", mappings.size(), states_.size());
}

DiscoveryManager::~DiscoveryManager() { stop(); }

void DiscoveryManager::start() {
    if (states_.empty()) return;

    // Spawn the dispatch thread before attaching any callbacks so the initial
    // evaluateMappings() already has a consumer.
    dispatch_thread_ = std::thread([this]() { this->dispatchLoop(); });

    bool need_dds_discovery = false;
    for (const auto& s : states_) {
        auto src = s.mapping.cmd_template.source;
        auto snk = s.mapping.cmd_template.sink;
        if (src == ProtocolType::DDS || snk == ProtocolType::DDS) {
            need_dds_discovery = true;
            break;
        }
    }

    if (need_dds_discovery && dds_participant_) {
        disc_listener_ = std::make_unique<DiscoveryListener>(this);
        dds_participant_->set_listener(disc_listener_.get(), eprosima::fastdds::dds::StatusMask::none());
    }

    int handler_count = 0;
    for (const auto& s : states_) {
        if (s.mapping.cmd_template.source == ProtocolType::SOMEIP) {
            const auto& cmd = s.mapping.cmd_template;
            if (cmd.service_id.has_value() && cmd.instance_id.has_value()) {
                vsomeip::service_t sid = cmd.service_id.value();
                vsomeip::instance_t iid = cmd.instance_id.value();

                someip_app_->request_service(sid, iid);
                someip_app_->register_availability_handler(
                    sid, iid, [this](vsomeip::service_t s, vsomeip::instance_t i, bool available) {
                        this->onSomeipAvailability(s, i, available);
                    });
                handler_count++;
            }
        }
    }
    log_->debug("Start: SOME/IP source availability handlers registered ({} service(s))", handler_count);

    // SOME/IP sink discovery: act as server up-front so external clients can
    // find us via SD. The tracker holds (one per sink=SOMEIP mapping) keep the
    // offers alive across createBridge / deleteBridge cycles.
    int sink_handler_count = 0;
    if (someip_app_ && someip_tracker_) {
        for (const auto& s : states_) {
            const auto& cmd = s.mapping.cmd_template;
            if (cmd.sink != ProtocolType::SOMEIP) continue;
            if (!cmd.service_id.has_value() || !cmd.instance_id.has_value() || !cmd.eventgroup_id.has_value() ||
                !cmd.event_id.has_value()) {
                log_->warn("[SOMEIP sink] mapping missing S/I/G/E IDs — skipping discovery hold");
                continue;
            }
            const vsomeip::service_t sid = cmd.service_id.value();
            const vsomeip::instance_t iid = cmd.instance_id.value();
            const vsomeip::eventgroup_t gid = cmd.eventgroup_id.value();
            const vsomeip::event_t eid = cmd.event_id.value();
            const SomeipEventgroupKey eg_key{sid, iid, gid};

            bool is_new_service, is_new_instance, is_new_group, is_new_event;
            someip_tracker_->addServerResource(sid, iid, gid, eid, is_new_service, is_new_instance, is_new_group,
                                               is_new_event);
            someip_sink_holds_.emplace_back(sid, iid, gid, eid);

            if (is_new_event) {
                std::set<vsomeip::eventgroup_t> groups{gid};
                someip_app_->offer_event(sid, iid, eid, groups);
            }
            if (is_new_instance) {
                someip_app_->offer_service(sid, iid);
            }

            if (someip_sub_handler_keys_.insert(eg_key).second) {
                // Use the vsomeip_sec_client_t-aware overload — the legacy
                // uid/gid version is deprecated in vsomeip 3.5.8.
                vsomeip::subscription_handler_sec_t handler =
                    [this, sid, iid, gid](vsomeip::client_t client, const vsomeip_sec_client_t*, const std::string&,
                                          bool subscribed) -> bool {
                    this->onSomeipSubscription(client, sid, iid, gid, subscribed);
                    return true;
                };
                someip_app_->register_subscription_handler(sid, iid, gid, handler);
                ++sink_handler_count;
            }
        }
    }
    log_->debug("Start: SOME/IP sink subscription handlers registered ({} eventgroup(s))", sink_handler_count);

    // For each (role, zenoh_key) we care about, declare one liveliness
    // subscriber on "@mpg/<role>/*/<zenoh_key>". PUT = peer declared the role,
    // DELETE = peer dropped. history=true picks up pre-existing tokens.
    int zenoh_sub_count = 0;
    if (zenoh_session_) {
        std::set<std::pair<std::string, std::string>> wanted;
        for (const auto& s : states_) {
            const auto& cmd = s.mapping.cmd_template;
            if (cmd.source == ProtocolType::ZENOH && !cmd.zenoh_key.empty()) {
                wanted.emplace(mpg::zenoh_liveliness::kRolePub, cmd.zenoh_key);
            }
            if (cmd.sink == ProtocolType::ZENOH && !cmd.zenoh_key.empty()) {
                wanted.emplace(mpg::zenoh_liveliness::kRoleSub, cmd.zenoh_key);
            }
        }

        for (const auto& [role, zkey] : wanted) {
            std::string sub_key = mpg::zenoh_liveliness::make_subscribe_key(role, zkey);

            auto on_sample = [this, role, zkey](const zenoh::Sample& sample) {
                if (!running_.load(std::memory_order_acquire)) return;
                const std::string_view matched = sample.get_keyexpr().as_string_view();
                const std::string_view zid = mpg::zenoh_liveliness::parse_zid(matched);
                if (zid.empty()) {
                    log_->warn("[ZenohLive] Malformed liveliness key: '{}'", std::string(matched));
                    return;
                }
                if (isSelfZid(zid)) return;
                const bool alive = (sample.get_kind() == Z_SAMPLE_KIND_PUT);
                if (role == mpg::zenoh_liveliness::kRolePub) {
                    onZenohPublisherDiscovered(zkey, alive);
                } else {
                    onZenohSubscriberDiscovered(zkey, alive);
                }
            };
            auto on_drop = []() {};

            try {
                zenoh::Session::LivelinessSubscriberOptions opts;
                opts.history = true;
                auto sub = zenoh_session_->liveliness_declare_subscriber(sub_key, std::move(on_sample),
                                                                         std::move(on_drop), std::move(opts));
                liveliness_subs_.push_back(std::move(sub));
                ++zenoh_sub_count;
            } catch (const std::exception& e) {
                log_->error("Start: Liveliness subscriber declare failed for '{}': {}", sub_key, e.what());
            }
        }
    }
    log_->debug("Start: Zenoh liveliness subscribers registered ({} key(s))", zenoh_sub_count);

    // Explicit initial evaluation for mappings whose conditions are already true.
    {
        std::lock_guard<std::mutex> lock(mutex_);
        evaluateMappings();
    }
}

void DiscoveryManager::stop() {
    if (!running_.exchange(false)) return;
    log_->debug("Shutdown: Started");

    // We deliberately do NOT call dds_participant_->set_listener(nullptr).
    // Fast-DDS 2.14 serializes set_listener against in-flight discovery
    // callbacks via an internal listener mutex; those callbacks acquire our
    // mutex_ and then re-enter Fast-DDS through core_->deleteBridge, which
    // can ladder back into the same internal mutex and deadlock main.
    // Instead: callbacks check running_ and early-return, and the listener
    // object is destroyed only after the participant (see GatewayCore Phase 3).

    if (someip_app_) {
        for (const auto& s : states_) {
            if (s.mapping.cmd_template.source == ProtocolType::SOMEIP) {
                const auto& cmd = s.mapping.cmd_template;
                if (cmd.service_id.has_value() && cmd.instance_id.has_value()) {
                    vsomeip::service_t sid = cmd.service_id.value();
                    vsomeip::instance_t iid = cmd.instance_id.value();
                    someip_app_->unregister_availability_handler(sid, iid);
                    someip_app_->release_service(sid, iid);
                }
            }
        }
    }

    // Release SOME/IP sink discovery holds + unregister handlers. We do not
    // proactively call stop_offer_service — the tracker decides based on
    // remaining ref counts; leftover offers are torn down by SomeipServer::stop.
    if (someip_app_ && someip_tracker_) {
        for (const auto& eg : someip_sub_handler_keys_) {
            const auto& [sid, iid, gid] = eg;
            someip_app_->unregister_subscription_handler(sid, iid, gid);
        }
        someip_sub_handler_keys_.clear();

        for (const auto& hold : someip_sink_holds_) {
            const auto& [sid, iid, gid, eid] = hold;
            bool should_stop_event, should_stop_instance, should_remove_service;
            (void)someip_tracker_->removeServerResource(sid, iid, gid, eid, should_stop_event, should_stop_instance,
                                                       should_remove_service);
            if (should_stop_event) someip_app_->stop_offer_event(sid, iid, eid);
            if (should_stop_instance) someip_app_->stop_offer_service(sid, iid);
        }
        someip_sink_holds_.clear();
    }

    // Dropping liveliness subscribers blocks until in-flight callbacks return;
    // combined with the running_=false check at the head of each callback this
    // guarantees no new mutex_ acquisition by the Zenoh thread after this point.
    liveliness_subs_.clear();

    { std::lock_guard<std::mutex> lock(mutex_); }

    // Wake the dispatch thread so it can observe running_=false and exit.
    // Queued actions are dropped — handlers and the participant will be torn
    // down by GatewayCore::shutdown() shortly anyway.
    action_cv_.notify_all();
    if (dispatch_thread_.joinable()) {
        dispatch_thread_.join();
    }
    log_->debug("Shutdown: Complete");
}

bool DiscoveryManager::isSelfGuid(const eprosima::fastrtps::rtps::GuidPrefix_t& prefix) const {
    return prefix == self_guid_prefix_;
}

bool DiscoveryManager::isSelfZid(std::string_view zid_hex) const {
    return !self_zid_hex_.empty() && zid_hex == self_zid_hex_;
}

bool DiscoveryManager::isSelfSomeipClient(vsomeip::client_t client) const {
    if (!someip_app_) return false;
    return client == someip_app_->get_client();
}

bool DiscoveryManager::isSelfOfferedService(vsomeip::service_t sid, vsomeip::instance_t iid) const {
    if (!someip_tracker_) return false;
    std::lock_guard<std::mutex> lock(someip_tracker_->mutex);
    auto svc_it = someip_tracker_->services.find(sid);
    if (svc_it == someip_tracker_->services.end()) return false;
    auto inst_it = svc_it->second.instances.find(iid);
    if (inst_it == svc_it->second.instances.end()) return false;
    return inst_it->second.server_count > 0;
}

void DiscoveryManager::onDdsWriterDiscovered(const std::string& topic, bool alive) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!running_.load(std::memory_order_acquire)) return;
    log_->info("[DDS Writer] {}: '{}'", alive ? "Discovered" : "Removed", topic);

    bool changed = false;
    for (size_t i = 0; i < states_.size(); ++i) {
        auto& state = states_[i];
        if (state.mapping.cmd_template.source != ProtocolType::DDS) continue;
        if (topic != state.mapping.cmd_template.topic) continue;
        if (state.source_condition != alive) {
            state.source_condition = alive;
            changed = true;
        }
    }
    if (changed) evaluateMappings();
}

void DiscoveryManager::onDdsReaderDiscovered(const std::string& topic, bool alive) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!running_.load(std::memory_order_acquire)) return;
    log_->info("[DDS Reader] {}: '{}'", alive ? "Discovered" : "Removed", topic);

    bool changed = false;
    for (size_t i = 0; i < states_.size(); ++i) {
        auto& state = states_[i];
        if (state.mapping.cmd_template.sink != ProtocolType::DDS) continue;
        if (topic != state.mapping.cmd_template.topic) continue;
        if (state.sink_condition != alive) {
            state.sink_condition = alive;
            changed = true;
        }
    }
    if (changed) evaluateMappings();
}

void DiscoveryManager::onSomeipAvailability(vsomeip::service_t sid, vsomeip::instance_t iid, bool available) {
    if (!running_.load(std::memory_order_acquire)) return;

    if (available && isSelfOfferedService(sid, iid)) return;

    std::lock_guard<std::mutex> lock(mutex_);
    if (!running_.load(std::memory_order_acquire)) return;
    log_->info("[SOMEIP] Service 0x{:04x}:0x{:04x} {}", sid, iid, available ? "available" : "unavailable");

    bool changed = false;
    for (size_t i = 0; i < states_.size(); ++i) {
        auto& state = states_[i];
        if (state.mapping.cmd_template.source != ProtocolType::SOMEIP) continue;

        const auto& cmd = state.mapping.cmd_template;
        if (cmd.service_id.has_value() && cmd.instance_id.has_value() && cmd.service_id.value() == sid &&
            cmd.instance_id.value() == iid) {
            if (state.source_condition != available) {
                state.source_condition = available;
                changed = true;
            }
        }
    }
    if (changed) evaluateMappings();
}

// Fires from a vSomeIP thread when an external client (un)subscribes to one of
// the eventgroups we offered for sink discovery. Self-filtered by client_id
// in case the same gateway is also a SOMEIP source.
void DiscoveryManager::onSomeipSubscription(vsomeip::client_t client, vsomeip::service_t sid, vsomeip::instance_t iid,
                                            vsomeip::eventgroup_t gid, bool subscribed) {
    if (!running_.load(std::memory_order_acquire)) return;
    if (isSelfSomeipClient(client)) return;

    std::lock_guard<std::mutex> lock(mutex_);
    if (!running_.load(std::memory_order_acquire)) return;

    const SomeipEventgroupKey key{sid, iid, gid};
    int& count = someip_sink_sub_counts_[key];
    const int prev = count;
    if (subscribed) {
        ++count;
    } else if (count > 0) {
        --count;
    }
    log_->info("[SOMEIP Sub] client=0x{:04x} {} S:0x{:04x}:0x{:04x} G:0x{:04x} (count {}->{})", client,
               subscribed ? "subscribed to" : "unsubscribed from", sid, iid, gid, prev, count);

    const bool was_active = (prev > 0);
    const bool now_active = (count > 0);
    if (was_active == now_active) return;

    bool changed = false;
    for (size_t i = 0; i < states_.size(); ++i) {
        auto& state = states_[i];
        const auto& cmd = state.mapping.cmd_template;
        if (cmd.sink != ProtocolType::SOMEIP) continue;
        if (!cmd.service_id.has_value() || !cmd.instance_id.has_value() || !cmd.eventgroup_id.has_value()) continue;
        if (cmd.service_id.value() != sid || cmd.instance_id.value() != iid || cmd.eventgroup_id.value() != gid)
            continue;
        if (state.sink_condition != now_active) {
            state.sink_condition = now_active;
            changed = true;
        }
    }
    if (changed) evaluateMappings();
}

void DiscoveryManager::onZenohPublisherDiscovered(const std::string& zenoh_key, bool alive) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!running_.load(std::memory_order_acquire)) return;
    log_->info("[Zenoh Pub] {}: '{}'", alive ? "Discovered" : "Removed", zenoh_key);

    bool changed = false;
    for (size_t i = 0; i < states_.size(); ++i) {
        auto& state = states_[i];
        if (state.mapping.cmd_template.source != ProtocolType::ZENOH) continue;
        if (zenoh_key != state.mapping.cmd_template.zenoh_key) continue;
        if (state.source_condition != alive) {
            state.source_condition = alive;
            changed = true;
        }
    }
    if (changed) evaluateMappings();
}

void DiscoveryManager::onZenohSubscriberDiscovered(const std::string& zenoh_key, bool alive) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!running_.load(std::memory_order_acquire)) return;
    log_->info("[Zenoh Sub] {}: '{}'", alive ? "Discovered" : "Removed", zenoh_key);

    bool changed = false;
    for (size_t i = 0; i < states_.size(); ++i) {
        auto& state = states_[i];
        if (state.mapping.cmd_template.sink != ProtocolType::ZENOH) continue;
        if (zenoh_key != state.mapping.cmd_template.zenoh_key) continue;
        if (state.sink_condition != alive) {
            state.sink_condition = alive;
            changed = true;
        }
    }
    if (changed) evaluateMappings();
}

bool DiscoveryManager::validateMapping(Command& cmd, size_t mapping_idx) {
    int source_count = (cmd.source != ProtocolType::NONE) ? 1 : 0;
    int sink_count = (cmd.sink != ProtocolType::NONE) ? 1 : 0;
    int total_count = source_count + sink_count;

    if (total_count == 0) {
        log_->error("[Validate] Mapping[{}] FAILED: No source or sink specified", mapping_idx);
        return false;
    }
    if (cmd.qos == Qos::NONE) {
        log_->error("[Validate] Mapping[{}] FAILED: QoS not specified", mapping_idx);
        return false;
    }

    bool is_dds = (cmd.source == ProtocolType::DDS) || (cmd.sink == ProtocolType::DDS);
    if (!is_dds && !cmd.topic.empty()) {
        log_->error("[Validate] Mapping[{}] FAILED: DDS not selected but topic is set ('{}')", mapping_idx, cmd.topic);
        return false;
    }

    bool is_zenoh = (cmd.source == ProtocolType::ZENOH) || (cmd.sink == ProtocolType::ZENOH);
    if (!is_zenoh && !cmd.zenoh_key.empty()) {
        log_->error("[Validate] Mapping[{}] FAILED: Zenoh not selected but zenoh_key is set ('{}')", mapping_idx,
                    cmd.zenoh_key);
        return false;
    }

    bool is_someip = (cmd.source == ProtocolType::SOMEIP) || (cmd.sink == ProtocolType::SOMEIP);
    if (!is_someip && (cmd.service_id.has_value() || cmd.instance_id.has_value() || cmd.eventgroup_id.has_value() ||
                       cmd.event_id.has_value())) {
        log_->error("[Validate] Mapping[{}] FAILED: SOME/IP not selected but IDs are set", mapping_idx);
        return false;
    }

    if (is_dds && !cmd.topic.empty()) {
        std::string original_topic = cmd.topic;

        if (cmd.topic.front() == '/') {
            cmd.topic = "rt" + cmd.topic;
        } else if (cmd.topic.compare(0, 3, "rt/") != 0) {
            cmd.topic = "rt/" + cmd.topic;
        }
        if (original_topic != cmd.topic) {
            log_->debug("[Validate] Mapping[{}] Topic auto-formatted: '{}' -> '{}'", mapping_idx, original_topic,
                        cmd.topic);
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
                error_reasons.push_back("Empty segment detected");
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
            log_->error("[Validate] Mapping[{}] Invalid topic naming: '{}'", mapping_idx, cmd.topic);
            for (const auto& reason : error_reasons) {
                log_->error("  - {}", reason);
            }
            return false;
        }
    }

    return true;
}

// Caller already holds mutex_. We must NOT invoke core_->createBridge /
// deleteBridge directly: this can run on a Fast-DDS callback thread, and
// re-entering Fast-DDS from inside a participant listener callback deadlocks
// on the participant's internal listener mutex. Instead, mutate state and
// enqueue a BridgeAction for the dispatch thread.
void DiscoveryManager::evaluateMappings() {
    for (size_t i = 0; i < states_.size(); ++i) {
        auto& state = states_[i];
        bool all_met = state.source_condition && state.sink_condition;

        std::string unmet_flags;
        if (!state.source_condition) unmet_flags += "source";
        if (!state.sink_condition) {
            if (!unmet_flags.empty()) unmet_flags += ", ";
            unmet_flags += "sink";
        }

        if (all_met && !state.is_active) {
            log_->info("[Evaluate] Mapping[{}]: All conditions met -> createBridge() (queued)", i);
            state.is_active = true;
            enqueueAction(BridgeAction{state.mapping.cmd_template, /*is_create=*/true, i});
        } else if (!all_met && state.is_active) {
            log_->info("[Evaluate] Mapping[{}]: Conditions lost [{}] -> deleteBridge() (queued)", i, unmet_flags);
            state.is_active = false;
            enqueueAction(BridgeAction{state.mapping.cmd_template, /*is_create=*/false, i});
        }
    }
}

void DiscoveryManager::enqueueAction(BridgeAction&& action) {
    {
        std::lock_guard<std::mutex> qlock(action_queue_mutex_);
        action_queue_.push_back(std::move(action));
    }
    action_cv_.notify_one();
}

void DiscoveryManager::dispatchLoop() {
    while (true) {
        BridgeAction action;
        {
            std::unique_lock<std::mutex> qlock(action_queue_mutex_);
            action_cv_.wait(qlock, [this]() {
                return !action_queue_.empty() || !running_.load(std::memory_order_acquire);
            });
            if (!running_.load(std::memory_order_acquire) && action_queue_.empty()) {
                break;
            }
            action = std::move(action_queue_.front());
            action_queue_.pop_front();
        }
        // Run outside the queue mutex and outside any Fast-DDS callback thread.
        // create/deleteBridge take GatewayCore::mutex_ and may freely re-enter
        // Fast-DDS / Zenoh / vSomeIP.
        if (action.is_create) {
            core_->createBridge(action.cmd);
        } else {
            core_->deleteBridge(action.cmd);
        }
    }
}

DiscoveryManager::DiscoveryListener::DiscoveryListener(DiscoveryManager* manager) : manager_(manager) {}

void DiscoveryManager::DiscoveryListener::on_publisher_discovery(
    eprosima::fastdds::dds::DomainParticipant* /*participant*/, eprosima::fastrtps::rtps::WriterDiscoveryInfo&& info) {
    // The listener stays attached until the participant is destroyed (see the
    // long note in DiscoveryManager::stop). Bail out early during shutdown.
    if (!manager_->running_.load(std::memory_order_acquire)) return;

    if (manager_->isSelfGuid(info.info.guid().guidPrefix)) return;

    std::string topic = info.info.topicName().to_string();

    if (info.status == eprosima::fastrtps::rtps::WriterDiscoveryInfo::DISCOVERED_WRITER) {
        manager_->onDdsWriterDiscovered(topic, true);
    } else if (info.status == eprosima::fastrtps::rtps::WriterDiscoveryInfo::REMOVED_WRITER) {
        manager_->onDdsWriterDiscovered(topic, false);
    }
}

void DiscoveryManager::DiscoveryListener::on_subscriber_discovery(
    eprosima::fastdds::dds::DomainParticipant* /*participant*/, eprosima::fastrtps::rtps::ReaderDiscoveryInfo&& info) {
    if (!manager_->running_.load(std::memory_order_acquire)) return;

    if (manager_->isSelfGuid(info.info.guid().guidPrefix)) return;

    std::string topic = info.info.topicName().to_string();

    if (info.status == eprosima::fastrtps::rtps::ReaderDiscoveryInfo::DISCOVERED_READER) {
        manager_->onDdsReaderDiscovered(topic, true);
    } else if (info.status == eprosima::fastrtps::rtps::ReaderDiscoveryInfo::REMOVED_READER) {
        manager_->onDdsReaderDiscovered(topic, false);
    }
}
