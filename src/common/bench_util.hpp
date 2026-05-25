#ifndef COMMON_BENCH_UTIL_HPP
#define COMMON_BENCH_UTIL_HPP

#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <vector>

// Bench-mode in-payload header (32B = 2 × 16-char zero-padded lowercase hex ASCII).
//
// Hex ASCII (not raw int64) is required because DDS CDR string fields truncate at
// null bytes when payloads route through std_msgs::String / sequence<octet>; each
// slot is always a null-safe printable 16-char window.
//
//   [ 0..15]  SEQ_KEY  - (pair_id << 32) | local_seq, written by ext_pub
//   [16..31]  TS       - timestamp slot, reused along the path:
//                          ext_pub  stamps T_PUB_SEND
//                          gw_sink  overwrites with T_BYTES_LEAVE after pop
//
// Clock: std::chrono::steady_clock = CLOCK_MONOTONIC on Linux/glibc. Same epoch
// across processes on one host, so cross-process timestamps subtract directly.

namespace bench {

constexpr size_t SLOT_LEN = 16;
constexpr size_t NUM_SLOTS = 2;
constexpr size_t HEADER_LEN = SLOT_LEN * NUM_SLOTS;  // 32

enum class Slot : size_t {
    SEQ_KEY = 0,
    TS = 1,
};

inline int64_t now_ns() { return std::chrono::steady_clock::now().time_since_epoch().count(); }

// (pair_id, local_seq) <-> seq_key encoding.
inline uint64_t make_seq_key(uint32_t pair_id, uint32_t local_seq) {
    return (static_cast<uint64_t>(pair_id) << 32) | static_cast<uint64_t>(local_seq);
}
inline uint32_t decode_pair_id(uint64_t seq_key) { return static_cast<uint32_t>(seq_key >> 32); }
inline uint32_t decode_local_seq(uint64_t seq_key) { return static_cast<uint32_t>(seq_key & 0xFFFFFFFFu); }

// Write a uint64 into the given slot. Caller guarantees payload.size() >= HEADER_LEN.
inline void write_slot(std::vector<uint8_t>& payload, Slot slot, uint64_t value) {
    size_t offset = static_cast<size_t>(slot) * SLOT_LEN;
    char buf[SLOT_LEN + 1];
    std::snprintf(buf, sizeof(buf), "%016lx", value);
    std::memcpy(payload.data() + offset, buf, SLOT_LEN);
}

// Read a uint64 from the given slot. Returns 0 if payload is too small.
inline uint64_t read_slot(const std::vector<uint8_t>& payload, Slot slot) {
    size_t offset = static_cast<size_t>(slot) * SLOT_LEN;
    if (payload.size() < offset + SLOT_LEN) return 0;
    char buf[SLOT_LEN + 1];
    std::memcpy(buf, payload.data() + offset, SLOT_LEN);
    buf[SLOT_LEN] = '\0';
    return std::strtoull(buf, nullptr, 16);
}

// Prepend a fresh 32B header to payload. SEQ_KEY = seq_key, TS = T_PUB_SEND (now).
inline void prepend_header(std::vector<uint8_t>& payload, uint64_t seq_key) {
    payload.insert(payload.begin(), HEADER_LEN, static_cast<uint8_t>('0'));
    write_slot(payload, Slot::SEQ_KEY, seq_key);
    write_slot(payload, Slot::TS, static_cast<uint64_t>(now_ns()));
}

// Stamp current time into the given slot in-place.
inline void stamp(std::vector<uint8_t>& payload, Slot slot) {
    write_slot(payload, slot, static_cast<uint64_t>(now_ns()));
}

// Stamp an externally-captured timestamp into the given slot in-place.
inline void stamp_value(std::vector<uint8_t>& payload, Slot slot, int64_t ns) {
    write_slot(payload, slot, static_cast<uint64_t>(ns));
}

// Remove the 32B header from the front of payload.
inline void strip_header(std::vector<uint8_t>& payload) {
    if (payload.size() >= HEADER_LEN) {
        payload.erase(payload.begin(), payload.begin() + HEADER_LEN);
    }
}

}  // namespace bench

#endif  // COMMON_BENCH_UTIL_HPP
