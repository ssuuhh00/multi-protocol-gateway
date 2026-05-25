# Examples

A demo set that exercises all six protocol-conversion directions of the
gateway. The gateway binary and `gateway_demo` consume **the same mapping
JSON file**: the gateway prepares the bridges declared in the mapping, and
`gateway_demo` plays the role of the external endpoints — publishing on the
source side and subscribing on the sink side of each mapping.

The two processes are launched in **two separate terminals**. The gateway is
a long-running daemon; the demo is a one-shot process that sends N messages
and exits. Keeping them apart makes the data flow easier to follow. There is
no helper script.

## Directory

```
examples/
├── CMakeLists.txt          # Independent build for gateway_demo
├── src/                    # Demo sources
│   ├── demo.cpp            # Pairs source ExtPublisher + sink ExtSubscriber
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
source ~/ws/setup.sh

# Gateway
rm -rf ~/ws/build/gateway && mkdir -p ~/ws/build/gateway
cmake -B ~/ws/build/gateway -S ~/ws/src/multi-protocol-gateway
make -C ~/ws/build/gateway -j$(nproc)

# Demo
rm -rf ~/ws/build/examples && mkdir -p ~/ws/build/examples
cmake -B ~/ws/build/examples -S ~/ws/src/multi-protocol-gateway/examples
make -C ~/ws/build/examples -j$(nproc)
```

## Run — all six directions at once (recommended starting point)

Both binaries require **every config path (mapping, Zenoh, vSomeIP) to be
supplied via environment variables**. There are no compile-time fallbacks.

### Terminal 1 — gateway (routing master + listening peer)

```bash
source ~/ws/setup.sh
export GATEWAY_MAPPING_PATH=~/ws/src/multi-protocol-gateway/examples/mappings/all_directions.json
export ZENOH_CONFIG_PATH=~/ws/src/multi-protocol-gateway/examples/configs/gateway_zenoh.json5
export VSOMEIP_CONFIG_PATH=~/ws/src/multi-protocol-gateway/examples/configs/gateway_vsomeip.json
export VSOMEIP_APP_NAME=gateway_app
export LOG_LEVEL=trace   # to see per-stage [Worker:*] | Queue/Conv lines
export LOG_PATH=~/ws/src/multi-protocol-gateway/examples/logs/gateway.log

~/ws/build/gateway/multi_protocol_gateway
```

The gateway stays up until SIGINT/SIGTERM (`Ctrl+C` to stop).

### Terminal 2 — demo (attaching peer + attaching client)

```bash
source ~/ws/setup.sh
export GATEWAY_MAPPING_PATH=~/ws/src/multi-protocol-gateway/examples/mappings/all_directions.json
export ZENOH_CONFIG_PATH=~/ws/src/multi-protocol-gateway/examples/configs/demo_zenoh.json5
export VSOMEIP_CONFIG_PATH=~/ws/src/multi-protocol-gateway/examples/configs/demo_vsomeip.json
export VSOMEIP_APP_NAME=demo_app
export LOG_PATH=~/ws/src/multi-protocol-gateway/examples/logs/demo.log

~/ws/build/examples/gateway_demo 10 2 64   # msg_count, rate_hz, payload_bytes
```

Demo CLI: `gateway_demo [msg_count=10] [rate_hz=2] [payload_bytes=64]`. The
mapping path is read from `GATEWAY_MAPPING_PATH`, the same env var the
gateway uses.

The demo spawns six external-endpoint pairs (six publishers + six
subscribers, twelve nodes total), sends 10 messages per pair, then exits.
All six directions flow on the console concurrently:

```
[DEMO] [DDS->ZENOH]    -> SEND seq=0 bytes=64
[DEMO] [ZENOH->DDS]    -> SEND seq=0 bytes=64
[DEMO] [DDS->SOMEIP]   -> SEND seq=0 bytes=64
[DEMO] [SOMEIP->DDS]   -> SEND seq=0 bytes=64
[DEMO] [ZENOH->SOMEIP] -> SEND seq=0 bytes=64
[DEMO] [SOMEIP->ZENOH] -> SEND seq=0 bytes=64
[DEMO] [DDS->ZENOH]    <- RECV seq_key=0x0 bytes=64
[DEMO] [ZENOH->DDS]    <- RECV seq_key=0x100000000 bytes=64
...
[DEMO] [DDS->ZENOH]    total received: 10
[DEMO] [ZENOH->DDS]    total received: 10
[DEMO] [DDS->SOMEIP]   total received: 10
[DEMO] [SOMEIP->DDS]   total received: 10
[DEMO] [ZENOH->SOMEIP] total received: 10
[DEMO] [SOMEIP->ZENOH] total received: 10
>>> gateway_demo exited cleanly.
```

With `LOG_LEVEL=trace`, per-stage latency lines from the gateway's handlers
show up too:

```
[DDS_SUB]  [Callback:rt/example_d2z]   | Conv: 0 us  | Size: 96 B
[ZEN_PUB]  [Worker:example/d2z]        | Queue: 7 us | Size: 96 B
```

## Run a single direction

To watch one direction at a time, swap only `GATEWAY_MAPPING_PATH` in both
terminals — the Zenoh/vSomeIP configs stay the same:

```bash
# Both terminals: change only GATEWAY_MAPPING_PATH
export GATEWAY_MAPPING_PATH=~/ws/src/multi-protocol-gateway/examples/mappings/dds_to_zenoh.json
~/ws/build/gateway/multi_protocol_gateway     # terminal 1
~/ws/build/examples/gateway_demo              # terminal 2
```

Available mappings:
`all_directions | dds_to_zenoh | zenoh_to_dds | dds_to_someip | someip_to_dds | zenoh_to_someip | someip_to_zenoh`

## Design notes

- Every example mapping has `bench_mode: true`, so each payload carries a
  32-byte latency header (2 slots of 16-char zero-padded hex ASCII: SEQ_KEY
  and TS — see `src/common/bench_util.hpp`). The demo prints `seq_key` from
  the header so you can visually match sent and received messages. Set
  `bench_mode: false` to remove the header.
- The SOMEIP demo delegates routing to the gateway via `routing: gateway_app`
  in `configs/demo_vsomeip.json`, so **the gateway must be up first** —
  start terminal 1, then terminal 2.
- `gateway_demo` is a **separate process** from the gateway. The gateway's
  DiscoveryManager detects each ExtDdsPub/Sub's DDS Writer/Reader, the
  ExtZenohPub/Sub's Liveliness tokens, and the ExtSomeipServer/Client's
  offer/findservice — and creates the bridges accordingly. When the demo
  exits, those discovery conditions go away and the gateway tears the
  bridges down (the gateway itself stays alive).
