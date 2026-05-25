#include "common/resource_tracker.hpp"

bool DDSResourceTracker::hasPubTopic(const std::string& topic) const {
    std::lock_guard<std::mutex> lock(mutex);
    return pub_topics.find(topic) != pub_topics.end();
}

void DDSResourceTracker::addPubTopic(const std::string& topic) {
    std::lock_guard<std::mutex> lock(mutex);
    pub_topics.insert(topic);
}

void DDSResourceTracker::removePubTopic(const std::string& topic) {
    std::lock_guard<std::mutex> lock(mutex);
    pub_topics.erase(topic);
}

int DDSResourceTracker::addSubTopic(const std::string& topic) {
    std::lock_guard<std::mutex> lock(mutex);
    return ++sub_topic_refs[topic];
}

int DDSResourceTracker::removeSubTopic(const std::string& topic) {
    std::lock_guard<std::mutex> lock(mutex);
    auto it = sub_topic_refs.find(topic);
    if (it == sub_topic_refs.end()) {
        return 0;
    }
    it->second--;
    if (it->second <= 0) {
        sub_topic_refs.erase(it);
        return 0;
    }
    return it->second;
}

eprosima::fastdds::dds::Topic* DDSResourceTracker::getTopicAndAddRef(const std::string& topic_name) {
    std::lock_guard<std::mutex> lock(mutex);
    auto it = topics.find(topic_name);
    if (it == topics.end()) {
        return nullptr;
    }
    topic_ref_counts[topic_name]++;
    return it->second;
}

void DDSResourceTracker::registerTopic(const std::string& topic_name, eprosima::fastdds::dds::Topic* topic) {
    std::lock_guard<std::mutex> lock(mutex);
    topics[topic_name] = topic;
    topic_ref_counts[topic_name] = 1;
}

int DDSResourceTracker::removeTopicRef(const std::string& topic_name, eprosima::fastdds::dds::Topic*& out_topic) {
    std::lock_guard<std::mutex> lock(mutex);
    out_topic = nullptr;

    auto ref_it = topic_ref_counts.find(topic_name);
    if (ref_it == topic_ref_counts.end()) {
        return 0;
    }

    ref_it->second--;
    int remaining = ref_it->second;

    if (remaining <= 0) {
        auto topic_it = topics.find(topic_name);
        if (topic_it != topics.end()) {
            out_topic = topic_it->second;
            topics.erase(topic_it);
        }
        topic_ref_counts.erase(ref_it);
    }

    return remaining;
}

eprosima::fastdds::dds::Topic* DDSResourceTracker::getTopic(const std::string& topic_name) const {
    std::lock_guard<std::mutex> lock(mutex);
    auto it = topics.find(topic_name);
    if (it == topics.end()) {
        return nullptr;
    }
    return it->second;
}

int DDSResourceTracker::getTopicRefCount(const std::string& topic_name) const {
    std::lock_guard<std::mutex> lock(mutex);
    auto it = topic_ref_counts.find(topic_name);
    if (it == topic_ref_counts.end()) {
        return 0;
    }
    return it->second;
}

bool ZenohResourceTracker::hasPubKey(const std::string& key) const {
    std::lock_guard<std::mutex> lock(mutex);
    return pub_keys.find(key) != pub_keys.end();
}

void ZenohResourceTracker::addPubKey(const std::string& key) {
    std::lock_guard<std::mutex> lock(mutex);
    pub_keys.insert(key);
}

void ZenohResourceTracker::removePubKey(const std::string& key) {
    std::lock_guard<std::mutex> lock(mutex);
    pub_keys.erase(key);
}

int ZenohResourceTracker::addSubKey(const std::string& key) {
    std::lock_guard<std::mutex> lock(mutex);
    return ++sub_key_refs[key];
}

int ZenohResourceTracker::removeSubKey(const std::string& key) {
    std::lock_guard<std::mutex> lock(mutex);
    auto it = sub_key_refs.find(key);
    if (it == sub_key_refs.end()) {
        return 0;
    }
    it->second--;
    if (it->second <= 0) {
        sub_key_refs.erase(it);
        return 0;
    }
    return it->second;
}

void SomeipResourceTracker::addServerResource(uint16_t service_id, uint16_t instance_id, uint16_t eventgroup_id,
                                              uint16_t event_id, bool& is_new_service, bool& is_new_instance,
                                              bool& is_new_group, bool& is_new_event) {
    std::lock_guard<std::mutex> lock(mutex);

    auto& svc = services[service_id];
    is_new_service = (svc.server_count == 0);
    svc.server_count++;

    auto& inst = svc.instances[instance_id];
    is_new_instance = (inst.server_count == 0);
    inst.server_count++;

    auto& grp = inst.groups[eventgroup_id];
    is_new_group = (grp.server_count == 0);
    grp.server_count++;

    auto& evt = grp.events[event_id];
    is_new_event = (evt.server_count == 0);
    evt.server_count++;
}

bool SomeipResourceTracker::removeServerResource(uint16_t service_id, uint16_t instance_id, uint16_t eventgroup_id,
                                                 uint16_t event_id, bool& should_stop_event, bool& should_stop_instance,
                                                 bool& should_remove_service) {
    std::lock_guard<std::mutex> lock(mutex);

    should_stop_event = false;
    should_stop_instance = false;
    should_remove_service = false;

    auto svc_it = services.find(service_id);
    if (svc_it == services.end()) return false;
    auto inst_it = svc_it->second.instances.find(instance_id);
    if (inst_it == svc_it->second.instances.end()) return false;
    auto grp_it = inst_it->second.groups.find(eventgroup_id);
    if (grp_it == inst_it->second.groups.end()) return false;
    auto evt_it = grp_it->second.events.find(event_id);
    if (evt_it == grp_it->second.events.end()) return false;

    evt_it->second.server_count--;
    if (evt_it->second.server_count <= 0) should_stop_event = true;
    if (evt_it->second.server_count <= 0 && evt_it->second.client_count <= 0) {
        grp_it->second.events.erase(evt_it);
    }

    // Group has no server-side vsomeip API, so no stop flag here — just
    // collapse the entry once both sides + child events are empty.
    grp_it->second.server_count--;
    if (grp_it->second.server_count <= 0 && grp_it->second.client_count <= 0 && grp_it->second.events.empty()) {
        inst_it->second.groups.erase(grp_it);
    }

    inst_it->second.server_count--;
    if (inst_it->second.server_count <= 0) should_stop_instance = true;
    if (inst_it->second.server_count <= 0 && inst_it->second.client_count <= 0 && inst_it->second.groups.empty()) {
        svc_it->second.instances.erase(inst_it);
    }

    svc_it->second.server_count--;
    if (svc_it->second.server_count <= 0) should_remove_service = true;
    if (svc_it->second.server_count <= 0 && svc_it->second.client_count <= 0 && svc_it->second.instances.empty()) {
        services.erase(svc_it);
    }

    return true;
}

void SomeipResourceTracker::addClientResource(uint16_t service_id, uint16_t instance_id, uint16_t eventgroup_id,
                                              uint16_t event_id, bool& is_new_service, bool& is_new_instance,
                                              bool& is_new_group, bool& is_new_event) {
    std::lock_guard<std::mutex> lock(mutex);

    auto& svc = services[service_id];
    is_new_service = (svc.client_count == 0);
    svc.client_count++;

    auto& inst = svc.instances[instance_id];
    is_new_instance = (inst.client_count == 0);
    inst.client_count++;

    auto& grp = inst.groups[eventgroup_id];
    is_new_group = (grp.client_count == 0);
    grp.client_count++;

    auto& evt = grp.events[event_id];
    is_new_event = (evt.client_count == 0);
    evt.client_count++;
}

bool SomeipResourceTracker::removeClientResource(uint16_t service_id, uint16_t instance_id, uint16_t eventgroup_id,
                                                 uint16_t event_id, bool& should_release_event,
                                                 bool& should_unsubscribe, bool& should_release_instance,
                                                 bool& should_remove_service) {
    std::lock_guard<std::mutex> lock(mutex);

    should_release_event = false;
    should_unsubscribe = false;
    should_release_instance = false;
    should_remove_service = false;

    auto svc_it = services.find(service_id);
    if (svc_it == services.end()) return false;

    auto inst_it = svc_it->second.instances.find(instance_id);
    if (inst_it == svc_it->second.instances.end()) return false;

    auto grp_it = inst_it->second.groups.find(eventgroup_id);
    if (grp_it == inst_it->second.groups.end()) return false;

    auto evt_it = grp_it->second.events.find(event_id);
    if (evt_it == grp_it->second.events.end()) return false;

    evt_it->second.client_count--;
    if (evt_it->second.client_count <= 0) should_release_event = true;
    if (evt_it->second.server_count <= 0 && evt_it->second.client_count <= 0) {
        grp_it->second.events.erase(evt_it);
    }

    grp_it->second.client_count--;
    if (grp_it->second.client_count <= 0) should_unsubscribe = true;
    if (grp_it->second.server_count <= 0 && grp_it->second.client_count <= 0 && grp_it->second.events.empty()) {
        inst_it->second.groups.erase(grp_it);
    }

    inst_it->second.client_count--;
    if (inst_it->second.client_count <= 0) should_release_instance = true;
    if (inst_it->second.server_count <= 0 && inst_it->second.client_count <= 0 && inst_it->second.groups.empty()) {
        svc_it->second.instances.erase(inst_it);
    }

    svc_it->second.client_count--;
    if (svc_it->second.client_count <= 0) should_remove_service = true;
    if (svc_it->second.server_count <= 0 && svc_it->second.client_count <= 0 && svc_it->second.instances.empty()) {
        services.erase(svc_it);
    }

    return true;
}
