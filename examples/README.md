# Examples

A demo set that exercises all six protocol-conversion directions of the
gateway. The gateway binary and `gateway_demo` consume **the same mapping
JSON file**: the gateway prepares the bridges declared in the mapping, and
`gateway_demo` plays the role of the external endpoints — publishing on the
source side and subscribing on the sink side of each mapping.

Everything here runs on **one host**. The two processes are launched in
**two separate terminals**: the gateway is a long-running daemon, the demo is
a one-shot process that sends N messages and exits. Keeping them apart makes
the data flow easier to follow.

## Directory

```
examples/
├── CMakeLists.txt          # Independent build for gateway_demo
├── env.sh                  # Sets up one terminal. Source it, don't run it
├── src/                    # Demo sources
│   ├── demo.cpp            # Pairs source ExtPublisher + sink ExtSubscriber
│   ├── queue_policy_test.cpp  # Prints the QoS spec sheet; also self-checks
│   └── ext_nodes/          # External-endpoint wrappers
├── configs/                # Runtime config for both gateway and demo
│   ├── gateway_vsomeip.json  # routing master, app=gateway_app
│   ├── gateway_zenoh.json5   # listen tcp/0.0.0.0:7447
│   ├── demo_vsomeip.json     # attach to gateway_app, app=demo_app
│   └── demo_zenoh.json5      # connect tcp/127.0.0.1:7447
└── mappings/               # One-direction mappings + an all-in-one
    ├── dds_to_zenoh.json
    ├── zenoh_to_dds.json
    ├── dds_to_someip.json
    ├── someip_to_dds.json
    ├── zenoh_to_someip.json
    ├── someip_to_zenoh.json
    └── all_directions.json   # all 6 directions in one file
```

## Build

```bash
# Gateway
cmake -B build -S . && make -C build -j$(nproc)

# Demo and the spec-sheet test
cmake -B build/examples -S examples && make -C build/examples -j$(nproc)
```

`zenohc` must be built with `-DZENOHC_BUILD_WITH_UNSTABLE_API=ON`; both
CMakeLists check for it and stop if it is missing. The QoS policies that ride
on Zenoh's Advanced Pub/Sub are compiled out without it.

## Run — all six directions at once (recommended starting point)

Both binaries require **every config path (mapping, Zenoh, vSomeIP) to be
supplied via environment variables**. There are no compile-time fallbacks.
`env.sh` sets one terminal's worth of them; read it if you would rather set
them by hand.

### Terminal 1 — gateway (routing master + listening peer)

```bash
source examples/env.sh gateway all_directions
$MPG_BIN_GW
```

The gateway stays up until SIGINT/SIGTERM (`Ctrl+C` to stop).

### Terminal 2 — demo (attaching peer + attaching client)

```bash
source examples/env.sh demo all_directions
$MPG_BIN_DEMO --count 150 --rate 50
```

The demo spawns six external-endpoint pairs (six publishers + six
subscribers, twelve nodes total) and reports each direction as it finishes:

```
[DEMO] [DDS->ZENOH] sent 150 / received 150
[DEMO] [ZENOH->DDS] sent 150 / received 150
[DEMO] [DDS->SOMEIP] sent 150 / received 150
[DEMO] [SOMEIP->DDS] sent 150 / received 150
[DEMO] [ZENOH->SOMEIP] sent 150 / received 150
[DEMO] [SOMEIP->ZENOH] sent 150 / received 150
>>> gateway_demo exited cleanly.
```

If your build trees are not under `<repo>/build`, set `MPG_BUILD_DIR` and
`MPG_DEMO_BUILD` before sourcing `env.sh`.

Demo CLI:

```
gateway_demo [count] [rate_hz] [payload_bytes]
             [--count N] [--rate HZ] [--payload BYTES]
             [--duration SEC|inf] [--settle-ms N] [--sink-delay-ms N]
             [--late-join-ms N] [--gap-at-ms N --gap-ms N]
```

`--duration inf` keeps the endpoints up so you can watch a running system
rather than a burst that ends before you look at it.

## Run a single direction

Swap only the mapping name in both terminals — the Zenoh/vSomeIP configs stay
the same:

```bash
source examples/env.sh gateway dds_to_zenoh    # terminal 1
source examples/env.sh demo    dds_to_zenoh    # terminal 2
```

Available mappings:
`all_directions | dds_to_zenoh | zenoh_to_dds | dds_to_someip | someip_to_dds | zenoh_to_someip | someip_to_zenoh`

## The `qos_policies` block

Every example mapping carries one:

```json
"qos_policies": {
  "reliability": "RELIABLE",
  "history": { "kind": "KEEP_LAST", "depth": 16 },
  "durability": "TRANSIENT_LOCAL",
  "liveliness": true
}
```

It is declared **once per mapping, not per leg** — it describes the data
crossing the bridge. What each leg is built with follows from the (policy,
direction) pair and is fixed, not negotiated; the gateway prints the rows a
mapping selected when it starts. `deadline` is the fifth policy and takes a
period in milliseconds; the examples leave it out because a sensible period
depends on your own send rate.

Omit the block entirely and the gateway behaves as if the feature were not
there — no QoS output, nothing configured.

Some policies have no counterpart in some middlewares. Where that happens the
gateway builds nothing for that leg and says so, naming the mechanism that is
missing — it does not refuse the mapping, and it does not accept the
declaration and quietly drop it. Running `all_directions` you will see two
such lines, for the SOME/IP sinks:

```
QoS: mapping[2] DDS->SOMEIP (d2s): RELIABILITY=RELIABLE HISTORY=KEEP_LAST(16)(!)
     LIVELINESS DURABILITY=TRANSIENT_LOCAL  (queue: max 16, oldest discarded)
QoS:   ! HISTORY not carried in d2s -- a vSomeIP event holds one value, not a
       depth; the declared depth bounds the gateway queue only
```

The gateway prints one line per mapping like the first, and a `!` line for
anything its direction does not carry. `LOG_LEVEL=debug` adds, for every
policy, what each leg was built with and why.

The full table, for all five policies in all six directions:

```bash
./build/examples/queue_policy_test --matrix
```

The declared `history` depth also bounds the bridge's own queue, with the
oldest discarded when it is full; `KEEP_ALL` or no declaration leaves it
unbounded. The gateway reports each queue's totals when the bridge goes away.

## Design notes

- Every example mapping has `bench_mode: true`, so each payload carries a
  32-byte latency header (2 slots of 16-char zero-padded hex ASCII: SEQ_KEY
  and TS — see `src/common/bench_util.hpp`). The demo prints `seq_key` from
  the header so you can visually match sent and received messages. Set
  `bench_mode: false` to remove the header.
- The SOMEIP demo delegates routing to the gateway via `routing: gateway_app`
  in `configs/demo_vsomeip.json`, so **the gateway must be up first** —
  start terminal 1, then terminal 2.
- `configs/*_vsomeip.json` give each offered service a `reliable` (TCP) port.
  vsomeip cannot invent one, and `"reliability": "RELIABLE"` on a SOME/IP leg
  selects the TCP endpoint; without the port the offer falls back to UDP and
  the gateway warns.
- `gateway_demo` is a **separate process** from the gateway. The gateway's
  DiscoveryManager detects each ExtDdsPub/Sub's DDS Writer/Reader, the
  ExtZenohPub/Sub's Liveliness tokens, and the ExtSomeipServer/Client's
  offer/subscribe — and creates the bridges accordingly. When the demo exits,
  those discovery conditions go away and the gateway tears the bridges down
  (the gateway itself stays alive).
