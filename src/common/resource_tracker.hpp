#ifndef RESOURCE_TRACKER_HPP
#define RESOURCE_TRACKER_HPP

#include <fastdds/dds/topic/Topic.hpp>
#include <map>
#include <mutex>
#include <set>
#include <string>

// Tracks DDS Topic objects shared between publisher and subscriber sides.
// pub_topics: at most one publisher per topic. sub_topic_refs: many subscribers
// allowed per topic (ref-counted). topics / topic_ref_counts manage the
// underlying Topic* lifetime.
struct DDSResourceTracker {
    mutable std::mutex mutex;

    std::set<std::string> pub_topics;
    std::map<std::string, int> sub_topic_refs;

    std::map<std::string, eprosima::fastdds::dds::Topic*> topics;
    std::map<std::string, int> topic_ref_counts;

    bool hasPubTopic(const std::string& topic) const;
    void addPubTopic(const std::string& topic);
    void removePubTopic(const std::string& topic);
    int addSubTopic(const std::string& topic);
    int removeSubTopic(const std::string& topic);

    eprosima::fastdds::dds::Topic* getTopicAndAddRef(const std::string& topic_name);
    void registerTopic(const std::string& topic_name, eprosima::fastdds::dds::Topic* topic);
    int removeTopicRef(const std::string& topic_name, eprosima::fastdds::dds::Topic*& out_topic);
    eprosima::fastdds::dds::Topic* getTopic(const std::string& topic_name) const;
    int getTopicRefCount(const std::string& topic_name) const;
};

// pub_keys: at most one publisher per key. sub_key_refs: many subscribers per key.
struct ZenohResourceTracker {
    mutable std::mutex mutex;
    std::set<std::string> pub_keys;
    std::map<std::string, int> sub_key_refs;

    bool hasPubKey(const std::string& key) const;
    void addPubKey(const std::string& key);
    void removePubKey(const std::string& key);
    int addSubKey(const std::string& key);
    int removeSubKey(const std::string& key);
};

// Hierarchy: Service -> Instance -> EventGroup -> Event. Server and Client
// keep independent ref counts so SomeipServer / SomeipClient on the same app
// can manage their own resources without stepping on each other; cleanup
// cascades upward when both counts reach zero at a level.
struct SomeipResourceTracker {
    mutable std::mutex mutex;

    struct EventInfo {
        int server_count = 0;
        int client_count = 0;
    };

    struct GroupInfo {
        int server_count = 0;
        int client_count = 0;
        std::map<uint16_t, EventInfo> events;
    };

    struct InstanceInfo {
        int server_count = 0;
        int client_count = 0;
        std::map<uint16_t, GroupInfo> groups;
    };

    struct ServiceInfo {
        int server_count = 0;
        int client_count = 0;
        std::map<uint16_t, InstanceInfo> instances;
    };

    std::map<uint16_t, ServiceInfo> services;

    void addServerResource(uint16_t service_id, uint16_t instance_id, uint16_t eventgroup_id, uint16_t event_id,
                           bool& is_new_service, bool& is_new_instance, bool& is_new_group, bool& is_new_event);
    bool removeServerResource(uint16_t service_id, uint16_t instance_id, uint16_t eventgroup_id, uint16_t event_id,
                              bool& should_stop_event, bool& should_stop_instance, bool& should_remove_service);

    void addClientResource(uint16_t service_id, uint16_t instance_id, uint16_t eventgroup_id, uint16_t event_id,
                           bool& is_new_service, bool& is_new_instance, bool& is_new_group, bool& is_new_event);
    bool removeClientResource(uint16_t service_id, uint16_t instance_id, uint16_t eventgroup_id, uint16_t event_id,
                              bool& should_release_event, bool& should_unsubscribe, bool& should_release_instance,
                              bool& should_remove_service);
};

#endif  // RESOURCE_TRACKER_HPP
