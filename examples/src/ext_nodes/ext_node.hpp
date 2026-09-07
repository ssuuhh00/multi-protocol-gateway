#ifndef EXTERNAL_EXT_NODE_HPP
#define EXTERNAL_EXT_NODE_HPP

#include <cstdint>
#include <functional>
#include <string>
#include <utility>
#include <vector>

#include "common/bench_util.hpp"

// External endpoints: the user-side ends of a gateway mapping.
//
// These are NOT gateway components. They stand in for the vehicle software that
// sits on either side of the bridge, and they exist for one reason the gateway
// cannot cover on its own: a QoS contract holds between two peers, so declaring
// a policy inside the gateway proves nothing unless the peer declares a
// matching one (on DDS, request/offer matching needs it; on Zenoh and SOME/IP
// nothing refuses, so the gap is silent). Liveliness is the clearest case -- a
// token nobody on the other side watches proves nothing, which is why the
// submitted version could leave it hardcoded true without anything visibly
// breaking.
//
// A second reason drives the identity fields below. The gateway physically
// cannot report who is on the other end: Command carries no name field, and the
// DDS discovery callback signature (topic, bool) drops the remote QoS before
// anyone can read it. Zenoh and SOME/IP have no such hook at all. So each
// endpoint states its own identity and its own declared policy, and reports
// them on its own channel.

// Which side of the bridge this endpoint sits on.
enum class ExtRole {
    PRODUCER,  // feeds the gateway's source leg
    CONSUMER,  // drains the gateway's sink leg
};

inline const char* to_string(ExtRole r) { return r == ExtRole::PRODUCER ? "producer" : "consumer"; }

// Identity of one external endpoint. Set by the demo from the mapping file.
struct EndpointSpec {
    std::string node_name;  // human name, e.g. "cam_front" -- shown in the monitor
    std::string address;    // what it binds to: DDS topic / Zenoh key / "S1234:E8001"
    ExtRole role = ExtRole::PRODUCER;
    uint32_t mapping_index = 0;  // index in the mapping JSON array; the monitor's row number
};

class ExtEndpoint {
public:
    virtual ~ExtEndpoint() = default;

    virtual void start() = 0;
    virtual void stop() = 0;

    // Middleware short name: "DDS" / "ZENOH" / "SOMEIP".
    virtual const char* protocol() const = 0;

    // What this endpoint actually handed its middleware, in one line. Not what
    // the config asked for -- what the API call received. The distinction is the
    // whole point: a policy the middleware silently ignored must not read as
    // applied. Rendered in the monitor's endpoint column.
    virtual std::string declared() const = 0;

    const EndpointSpec& spec() const { return spec_; }

protected:
    explicit ExtEndpoint(EndpointSpec spec) : spec_(std::move(spec)) {}
    EndpointSpec spec_;
    bool started_ = false;
};

// Produces messages into the gateway's source leg.
class ExtPublisher : public ExtEndpoint {
public:
    // seq_key = (pair_id << 32) | local_seq. payload_bytes excludes the 32B
    // bench header that prepend_header() adds.
    //
    // Returns T_PUB_SEND -- the stamp this call wrote into the header. The
    // caller must keep it: the gateway overwrites that slot, so by the time the
    // message reaches the subscriber the send time is gone from the wire. Pairing
    // the returned value with the subscriber's recv_ns gives an E2E measured
    // entirely on THIS host, which is the only kind that survives a two-host run
    // host, which is the only kind that survives a two-host run.
    virtual int64_t send(uint64_t seq_key, size_t payload_bytes) = 0;

    // Optional: called by send() once the bench header is in place and before the
    // message leaves, so the caller can write further slots into the payload BODY
    // (offset >= bench::HEADER_LEN). The gateway reads slots 0 and 1 only, so
    // anything written past 32 B rides through every conversion untouched.
    //
    // A teleoperation demo uses this to carry a cross-stream link: the command
    // a console emits names the camera frame the console had last received, and
    // how long it then waited before emitting.
    using StampFn = std::function<void(std::vector<uint8_t>& payload, uint64_t seq_key)>;
    void setStamp(StampFn fn) { stamp_ = std::move(fn); }

protected:
    using ExtEndpoint::ExtEndpoint;
    StampFn stamp_;
};

// Consumes messages out of the gateway's sink leg.
class ExtSubscriber : public ExtEndpoint {
public:
    // payload still carries the bench header; recv_ns is steady_clock, which
    // shares an epoch across processes on this host.
    using OnMessageFn = std::function<void(const std::vector<uint8_t>& payload, int64_t recv_ns)>;

    void setOnMessage(OnMessageFn fn) { on_message_ = std::move(fn); }

protected:
    using ExtEndpoint::ExtEndpoint;
    OnMessageFn on_message_;
};

#endif  // EXTERNAL_EXT_NODE_HPP
