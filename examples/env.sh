# Sets up one of the three demo terminals. Source it, don't run it.
#
#   source examples/env.sh gateway [mapping]
#   source examples/env.sh demo    [mapping]
#
# `mapping` is a name under examples/mappings/ without the .json
# (default: all_directions). The gateway and the demo must be given the SAME one.
# Everything here is single-host: the demo attaches to the gateway's vSomeIP
# routing master and connects to its Zenoh peer on 127.0.0.1.
#
# Every path the gateway and the demo need is passed by env; there are no
# compile-time defaults. That is why this file exists -- seven exports per
# terminal, typed three times, is how the wrong config gets used.
#
# Two variables it does not set for you:
#   MPG_BUILD_DIR   where the gateway was built (default: <repo>/build)
#   MPG_DEMO_BUILD  where the examples were built (default: $MPG_BUILD_DIR/examples,
#                   which is what the README's build commands produce)
# Set them before sourcing if your build trees live elsewhere.

_ex_role="${1:-}"
_ex_map="${2:-all_directions}"

# This file's directory, whether sourced by bash or zsh.
if [ -n "${BASH_SOURCE[0]:-}" ]; then
    EX="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
else
    EX="$(cd "$(dirname "${(%):-%x}")" && pwd)"
fi
_ex_repo="$(cd "$EX/.." && pwd)"

if [ -z "$_ex_role" ]; then
    echo "usage: source env.sh {gateway|demo} [mapping]"
    echo "mappings: $(ls "$EX/mappings" | sed 's/\.json$//' | tr '\n' ' ')"
    return 1 2>/dev/null || exit 1
fi

export MPG_BUILD_DIR="${MPG_BUILD_DIR:-$_ex_repo/build}"
export MPG_DEMO_BUILD="${MPG_DEMO_BUILD:-$MPG_BUILD_DIR/examples}"

_ex_file="$EX/mappings/${_ex_map}.json"
if [ ! -f "$_ex_file" ]; then
    echo "no such mapping: $_ex_file"
    return 1 2>/dev/null || exit 1
fi
export GATEWAY_MAPPING_PATH="$_ex_file"
mkdir -p "$EX/logs"

case "$_ex_role" in
    gateway)
        # The gateway is the vSomeIP routing master and the listening Zenoh peer.
        export ZENOH_CONFIG_PATH="$EX/configs/gateway_zenoh.json5"
        export VSOMEIP_CONFIG_PATH="${VSOMEIP_CONFIG_PATH:-$EX/configs/gateway_vsomeip.json}"
        export VSOMEIP_APP_NAME=gateway_app
        export LOG_LEVEL="${LOG_LEVEL:-info}"
        export LOG_PATH="$EX/logs/gateway.log"
        export MPG_BIN_GW="$MPG_BUILD_DIR/multi_protocol_gateway"
        echo "terminal 1 (gateway) ready -- mapping=${_ex_map}"
        echo "  run: \$MPG_BIN_GW"
        ;;
    demo)
        # The demo attaches to the gateway's routing master and connects to its
        # Zenoh peer, so both configs are the client-side pair.
        export ZENOH_CONFIG_PATH="$EX/configs/demo_zenoh.json5"
        export VSOMEIP_CONFIG_PATH="${VSOMEIP_CONFIG_PATH:-$EX/configs/demo_vsomeip.json}"
        export VSOMEIP_APP_NAME=demo_app
        export LOG_LEVEL="${LOG_LEVEL:-info}"
        export LOG_PATH="$EX/logs/demo.log"
        export MPG_BIN_DEMO="$MPG_DEMO_BUILD/gateway_demo"
        echo "terminal 2 (demo) ready -- mapping=${_ex_map}"
        echo "  run: \$MPG_BIN_DEMO --duration inf --rate 20"
        ;;
    *)
        echo "unknown role: $_ex_role  (gateway|demo)"
        return 1 2>/dev/null || exit 1
        ;;
esac

unset _ex_role _ex_map _ex_file _ex_repo
