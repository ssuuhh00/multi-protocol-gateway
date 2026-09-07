#include "someip_server.hpp"

#include <spdlog/sinks/stdout_color_sinks.h>

#include "common/bench_util.hpp"
#include "someip/someip_qos_apply.hpp"

SomeipServer::SomeipServer(std::shared_ptr<vsomeip::application> app, std::shared_ptr<SomeipResourceTracker> tracker,
                           ConnectionQueueMap* queue_map)
    : app_(app), tracker_(tracker), queue_map_(queue_map) {
    log_ = spdlog::get("SMP_SRV");
    if (!log_) {
        log_ = spdlog::stdout_color_mt("SMP_SRV");
        log_->set_level(spdlog::level::info);
    }

    if (!app_) {
        log_->critical("FATAL: vSomeIP application is nullptr");
        throw std::runtime_error("vSomeIP application is nullptr");
    }
}

SomeipServer::~SomeipServer() { stop(); }

void SomeipServer::stop() {
    if (!running_.exchange(false)) return;

    {
        std::lock_guard<std::mutex> lock(resource_mutex_);
        for (auto& [key, worker] : worker_threads_) {
            if (!worker) continue;
            worker->stop_flag->store(true);
            // Wake the blocked pop() with a poison pill. push_wakeup(), not
            // push(): a bounded queue would evict a real sample to admit a
            // sentinel and charge it to drop_total.
            if (auto q = queue_map_->get(key)) {
                q->push_wakeup();
            }
            if (worker->thread.joinable()) {
                worker->thread.join();
            }
        }
        worker_threads_.clear();
    }

    if (app_ && tracker_) {
        std::lock_guard<std::mutex> tracker_lock(tracker_->mutex);
        for (auto& [s_id, svc_info] : tracker_->services) {
            for (auto& [i_id, inst_info] : svc_info.instances) {
                if (inst_info.server_count <= 0) continue;
                for (auto& [g_id, group_info] : inst_info.groups) {
                    for (auto& [e_id, event_info] : group_info.events) {
                        if (event_info.server_count > 0) {
                            app_->stop_offer_event(s_id, i_id, e_id);
                        }
                    }
                }
                app_->stop_offer_service(s_id, i_id);
            }
        }
    }
}

bool SomeipServer::handleCreate(const Command& cmd) {
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

    std::lock_guard<std::mutex> lock(resource_mutex_);

    if (worker_threads_.count(conn_key)) {
        log_->warn("Creating: SKIPPED: Worker already exists for key '{}'", conn_key);
        return false;
    }

    bool is_new_service, is_new_instance, is_new_group, is_new_event;
    tracker_->addServerResource(s_id, i_id, g_id, e_id, is_new_service, is_new_instance, is_new_group, is_new_event);
    (void)is_new_service;
    (void)is_new_group;

    if (is_new_event) {
        std::set<vsomeip::eventgroup_t> groups{g_id};

        // offer_event has six optional arguments and this leg used to pass
        // none. Two of them carry policies, and only those two are filled: the
        // event type (ET_FIELD retains the current value for a late subscriber)
        // and the reliability (RT_RELIABLE selects the TCP endpoint). `_cycle`
        // is left at zero on purpose -- see someip_qos_apply.hpp.
        const auto ev_type = someip_qos::eventType(cmd.qos_policies);
        const auto rel = someip_qos::reliability(cmd.qos_policies);
        app_->offer_event(s_id, i_id, e_id, groups, ev_type, std::chrono::milliseconds::zero(), false, true, nullptr,
                          rel);

        if (!cmd.qos_policies.empty()) {
            log_->info("Creating: QoS on sink leg: {}", someip_qos::describe(cmd.qos_policies));
            if (ev_type == vsomeip::event_type_e::ET_FIELD) {
                // vsomeip keeps one event type per (service, instance, event)
                // and refuses a registration that disagrees ("Event
                // registration update failed"), leaving whichever type was
                // registered first in force. Measured on a single routing host:
                // a mismatched pair still streams normally -- what is not in
                // force is the type the later declaration asked for, and with
                // it the retained-value behaviour ET_FIELD is declared for. So
                // a durability declaration here is a statement about the peer
                // too, and both sides have to make it.
                log_->warn(
                    "Creating: ET_FIELD on E:0x{:04x} requires the subscribing client to request it as a field "
                    "too; if it requests ET_EVENT, vSomeIP refuses the second registration and the field "
                    "semantics are not the ones declared",
                    e_id);
            }
            if (rel == vsomeip::reliability_type_e::RT_RELIABLE) {
                // vsomeip cannot invent a TCP endpoint. If the service has none
                // declared, the offer silently falls back and the mapping is
                // BEST_EFFORT while claiming otherwise -- worth saying out loud
                // because it is the one place this leg can quietly under-deliver.
                log_->warn(
                    "Creating: RT_RELIABLE needs a TCP endpoint for S:0x{:04x} in VSOMEIP_CONFIG_PATH; "
                    "without one this event stays on UDP",
                    s_id);
            }
        }
    }

    // The "the SD offer is per instance" caveat for a declared liveliness is
    // raised by DiscoveryManager, not here: is_new_instance is false for every
    // sink mapping because DiscoveryManager pre-offers the instance for service
    // discovery, so this is not the place that can tell whether the instance
    // really carries more than one mapping.
    if (is_new_instance) {
        app_->offer_service(s_id, i_id);
    }

    queue_map_->create(conn_key);

    auto worker = std::make_unique<WorkerThread>();
    worker->stop_flag = std::make_shared<std::atomic<bool>>(false);
    worker->thread = std::thread(&SomeipServer::serverWorker, this, cmd, worker->stop_flag);
    worker_threads_[conn_key] = std::move(worker);

    log_->debug("Creating: SUCCESS: Server and worker started for {} (key: {})",
                formatSomeipId(s_id, i_id, g_id, e_id), conn_key);
    return true;
}

bool SomeipServer::handleDelete(const Command& cmd) {
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

    auto it = worker_threads_.find(conn_key);
    if (it != worker_threads_.end()) {
        it->second->stop_flag->store(true);
        if (auto q = queue_map_->get(conn_key)) {
            // push_wakeup(), not push(): see the note on ConnectionQueue.
            q->push_wakeup();
        } else {
            log_->error("Deleting: Queue not found for key '{}' - worker may hang!", conn_key);
        }
        if (it->second->thread.joinable()) {
            it->second->thread.join();
        }
        worker_threads_.erase(it);
        queue_map_->remove(conn_key);
    } else {
        log_->warn("Deleting: SKIPPED: No worker found for key '{}'", conn_key);
    }

    bool should_stop_event, should_stop_instance, should_remove_service;
    bool found = tracker_->removeServerResource(s_id, i_id, g_id, e_id, should_stop_event, should_stop_instance,
                                                should_remove_service);
    (void)should_remove_service;
    if (!found) {
        log_->warn("Deleting: SKIPPED: Resource hierarchy not found or already deleted");
        return false;
    }

    if (should_stop_event) {
        app_->stop_offer_event(s_id, i_id, e_id);
    }
    if (should_stop_instance) {
        app_->stop_offer_service(s_id, i_id);
    }

    log_->debug("Deleting: SUCCESS: Cleaned up {} (key: {})", formatSomeipId(s_id, i_id, g_id, e_id), conn_key);
    return true;
}

void SomeipServer::serverWorker(Command cmd, std::shared_ptr<std::atomic<bool>> stop_flag) {
    std::string conn_key = make_conn_key(cmd);

    vsomeip::service_t s_id = cmd.service_id.value();
    vsomeip::instance_t i_id = cmd.instance_id.value();
    vsomeip::event_t e_id = cmd.event_id.value();

    auto data_queue = queue_map_->get(conn_key);
    if (!data_queue) {
        log_->error("[Worker:S{:x}:E{:x}] FAILED: Queue not found for key '{}'", s_id, e_id, conn_key);
        return;
    }

    // NOTE: SOME/IP reliability is the transport: UDP is best-effort, TCP is
    //       reliable. offer_event() takes a reliability_type_e and handleCreate
    //       passes RT_RELIABLE when the sink leg declares it. What vsomeip
    //       cannot do is invent an endpoint: the service still needs a TCP port
    //       in VSOMEIP_CONFIG_PATH, and handleCreate warns when a mapping asks
    //       for RT_RELIABLE without one.

    while (running_ && !stop_flag->load()) {
        DataType data_box = data_queue->pop();
        if (!running_ || stop_flag->load() || data_box.data.empty()) break;

        // Bench (SOMEIP sink): no msg-object conversion. Stamp T_BYTES_LEAVE
        // + read seq_key before payload->set_data() moves the buffer.
        auto t_bytes_leave = std::chrono::steady_clock::now();
        auto queue_latency_us =
            std::chrono::duration_cast<std::chrono::microseconds>(t_bytes_leave - data_box.timestamp).count();
        // Hand the queue the dwell it just computed: one relaxed add, no extra
        // clock read. The queue owns the aggregate because it is the only thing
        // that also knows the depth the message waited behind.
        data_queue->record_dwell_us(static_cast<uint64_t>(queue_latency_us));

        uint64_t bench_seq_key = 0;
        if (cmd.bench_mode) {
            bench::stamp_value(data_box.data, bench::Slot::TS, t_bytes_leave.time_since_epoch().count());
            bench_seq_key = bench::read_slot(data_box.data, bench::Slot::SEQ_KEY);
        }

        size_t data_size = data_box.data.size();

        std::shared_ptr<vsomeip::payload> payload = vsomeip::runtime::get()->create_payload();
        // Move into vsomeip payload so it takes ownership without copying.
        payload->set_data(std::move(data_box.data));

        app_->notify(s_id, i_id, e_id, payload);

        if (cmd.bench_mode) {
            log_->trace("[Worker:S{:x}:E{:x}] | Seq: {} | Queue: {} us | Size: {} B",
                        s_id, e_id, bench_seq_key, queue_latency_us, data_size);
        } else {
            log_->trace("[Worker:S{:x}:E{:x}] | Queue: {} us | Size: {} B", s_id, e_id, queue_latency_us, data_size);
        }
    }
}

std::string SomeipServer::formatSomeipId(vsomeip::service_t s, vsomeip::instance_t i, vsomeip::eventgroup_t g,
                                         vsomeip::event_t e) {
    char buf[64];
    snprintf(buf, sizeof(buf), "S:0x%04x I:0x%04x G:0x%04x E:0x%04x", s, i, g, e);
    return std::string(buf);
}
