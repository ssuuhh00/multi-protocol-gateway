#include "someip_client.hpp"

#include <spdlog/sinks/stdout_color_sinks.h>

#include <chrono>
#include <set>

#include "common/bench_util.hpp"

SomeipClient::SomeipClient(std::shared_ptr<vsomeip::application> app, std::shared_ptr<SomeipResourceTracker> tracker,
                           ConnectionQueueMap* queue_map)
    : app_(app), tracker_(tracker), queue_map_(queue_map) {
    log_ = spdlog::get("SMP_CLI");
    if (!log_) {
        log_ = spdlog::stdout_color_mt("SMP_CLI");
        log_->set_level(spdlog::level::info);
    }

    if (!app_) {
        log_->critical("FATAL: vSomeIP application is nullptr");
        throw std::runtime_error("vSomeIP application is nullptr");
    }
}

SomeipClient::~SomeipClient() { stop(); }

void SomeipClient::stop() {
    if (!running_.exchange(false)) return;

    std::lock_guard<std::mutex> lock(resource_mutex_);

    if (!app_ || !tracker_) return;

    std::lock_guard<std::mutex> tracker_lock(tracker_->mutex);
    for (auto& [s_id, svc_info] : tracker_->services) {
        for (auto& [i_id, inst_info] : svc_info.instances) {
            if (inst_info.client_count <= 0) continue;
            for (auto& [g_id, group_info] : inst_info.groups) {
                if (group_info.client_count <= 0) continue;
                for (auto& [e_id, event_info] : group_info.events) {
                    if (event_info.client_count > 0) {
                        app_->unregister_message_handler(s_id, i_id, e_id);
                    }
                }
                app_->unsubscribe(s_id, i_id, g_id);
            }
            app_->release_service(s_id, i_id);
        }
    }
}

bool SomeipClient::handleCreate(const Command& cmd) {
    std::string conn_key = make_conn_key(cmd);

    if (!cmd.service_id.has_value() || !cmd.instance_id.has_value() || !cmd.event_id.has_value() ||
        !cmd.eventgroup_id.has_value()) {
        log_->error("Creating: FAILED: Missing required SOME/IP IDs (S/I/E/G)");
        return false;
    }

    vsomeip::service_t s_id = cmd.service_id.value();
    vsomeip::instance_t i_id = cmd.instance_id.value();
    vsomeip::eventgroup_t g_id = cmd.eventgroup_id.value();
    vsomeip::event_t e_id = cmd.event_id.value();

    // Create queue before taking resource_mutex_ so the callback can find it
    // without contending against handleCreate.
    queue_map_->create(conn_key);

    std::lock_guard<std::mutex> lock(resource_mutex_);

    bool is_new_service, is_new_instance, is_new_group, is_new_event;
    tracker_->addClientResource(s_id, i_id, g_id, e_id, is_new_service, is_new_instance, is_new_group, is_new_event);
    (void)is_new_service;

    if (is_new_event) {
        std::set<vsomeip::eventgroup_t> groups{g_id};
        app_->request_event(s_id, i_id, e_id, groups, vsomeip::event_type_e::ET_EVENT);
        app_->register_message_handler(
            s_id, i_id, e_id,
            std::bind(&SomeipClient::onMessageReceived, this, std::placeholders::_1, queue_map_, cmd, log_));
    }
    if (is_new_group) {
        app_->subscribe(s_id, i_id, g_id);
    }
    if (is_new_instance) {
        app_->request_service(s_id, i_id);
    }

    log_->debug("Creating: SUCCESS: Client started for {} (key: {})", formatSomeipId(s_id, i_id, g_id, e_id), conn_key);
    return true;
}

bool SomeipClient::handleDelete(const Command& cmd) {
    std::string conn_key = make_conn_key(cmd);

    if (!cmd.service_id.has_value() || !cmd.instance_id.has_value() || !cmd.event_id.has_value() ||
        !cmd.eventgroup_id.has_value()) {
        log_->error("Deleting: FAILED: Missing required SOME/IP IDs");
        return false;
    }

    vsomeip::service_t s_id = cmd.service_id.value();
    vsomeip::instance_t i_id = cmd.instance_id.value();
    vsomeip::eventgroup_t g_id = cmd.eventgroup_id.value();
    vsomeip::event_t e_id = cmd.event_id.value();

    std::lock_guard<std::mutex> lock(resource_mutex_);

    bool should_release_event, should_unsubscribe, should_release_instance, should_remove_service;
    bool found = tracker_->removeClientResource(s_id, i_id, g_id, e_id, should_release_event, should_unsubscribe,
                                                should_release_instance, should_remove_service);
    (void)should_remove_service;
    if (!found) {
        log_->warn("Deleting: SKIPPED: Resource hierarchy not found or already deleted");
        return false;
    }

    if (should_release_event) {
        app_->unregister_message_handler(s_id, i_id, e_id);
        app_->release_event(s_id, i_id, e_id);
    }
    if (should_unsubscribe) {
        app_->unsubscribe(s_id, i_id, g_id);
    }
    if (should_release_instance) {
        app_->release_service(s_id, i_id);
    }

    log_->debug("Deleting: SUCCESS: Cleaned up {} (key: {})", formatSomeipId(s_id, i_id, g_id, e_id), conn_key);
    return true;
}

void SomeipClient::onMessageReceived(const std::shared_ptr<vsomeip::message>& msg, ConnectionQueueMap* queue_map,
                                     Command cmd, std::shared_ptr<spdlog::logger> log) {
    vsomeip::service_t s_id = msg->get_service();
    vsomeip::event_t e_id = msg->get_method();

    auto t_recv_start = std::chrono::steady_clock::now();

    std::shared_ptr<vsomeip::payload> pl = msg->get_payload();
    if (!pl) {
        log->warn("[Callback:S{:x}:E{:x}] Received message with nullptr payload", s_id, e_id);
        return;
    }

    uint32_t size = pl->get_length();
    vsomeip::byte_t* data_ptr = pl->get_data();
    if (!data_ptr || size == 0) return;

    std::vector<unsigned char> raw_bytes(data_ptr, data_ptr + size);

    uint64_t bench_seq_key = 0;
    int64_t bench_src_mw_lumped_us = 0;
    if (cmd.bench_mode) {
        bench_seq_key = bench::read_slot(raw_bytes, bench::Slot::SEQ_KEY);
        const int64_t t_pub_send = static_cast<int64_t>(bench::read_slot(raw_bytes, bench::Slot::TS));
        bench_src_mw_lumped_us = (t_recv_start.time_since_epoch().count() - t_pub_send) / 1000;
    }

    std::string conn_key = make_conn_key(cmd);
    auto queue = queue_map->get(conn_key);
    if (!queue) {
        log->error("[Callback:S{:x}:E{:x}] Queue not found for key '{}' - data dropped!", s_id, e_id, conn_key);
        return;
    }

    queue->push(std::move(raw_bytes));

    if (cmd.bench_mode) {
        log->trace("[Callback:S{:x}:E{:x}] | Seq: {} | SrcMW: {} us | Size: {} B",
                   s_id, e_id, bench_seq_key, bench_src_mw_lumped_us, size);
    } else {
        log->trace("[Callback:S{:x}:E{:x}] | Size: {} B", s_id, e_id, size);
    }
}

std::string SomeipClient::formatSomeipId(vsomeip::service_t s, vsomeip::instance_t i, vsomeip::eventgroup_t g,
                                         vsomeip::event_t e) {
    char buf[64];
    snprintf(buf, sizeof(buf), "S:0x%04x I:0x%04x G:0x%04x E:0x%04x", s, i, g, e);
    return std::string(buf);
}
