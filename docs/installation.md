# Installation Guide

## System Requirements & Dependencies

| Component | Version | Description |
| :--- | :--- | :--- |
| **OS** | Ubuntu 24.04 LTS | Kernel 6.8+ / GCC 13.3.0 (also tested on Jetson Ubuntu 22.04 / JetPack 6.x — see note below) |
| **Language** | C++17 | Required for Zenoh C++ API & Modern DDS |
| **Build System** | CMake 3.28+ | Ubuntu 24.04 Default |
| **Memory Alloc** | Foonathan Memory v0.7-3 | Required dependency for Fast DDS |
| **DDS Serialization** | Fast CDR v2.2.5 | CDR serialization library |
| **DDS Core** | Fast DDS v2.14.5 | DDS implementation (eProsima) |
| **DDS Tool** | Fast-DDS-Gen v3.3.1 | IDL compiler (requires Java 17) |
| **vSomeIP** | vSomeIP v3.5.8 | COVESA SOME/IP (Boost 1.83 compatible) |
| **Zenoh** | Zenoh C/C++ v1.8.0 | Stable Release (requires Rust/Cargo) |
| **Logging** | spdlog v1.12.0 | Header-only, included as git submodule |

> **Jetson (Ubuntu 22.04 / JetPack 6.x) note.** The IEEE Access paper's
> Experiments 1–5 ran on a Jetson Orin Nano with Ubuntu 22.04. On 22.04 the
> apt-default `libboost-all-dev` is 1.74, but vSomeIP v3.5.8 requires Boost
> 1.83. Build Boost 1.83 from source under `$INSTALL_DIR` before Phase 3,
> then pass `-DBoost_NO_SYSTEM_PATHS=ON -DBOOST_ROOT=$INSTALL_DIR` to the
> vSomeIP cmake invocation in Phase 3 (Step 4). All other steps are
> identical to the Ubuntu 24.04 flow below.

---

## Phase 0: System Packages

```bash
#!/bin/bash

# System update
sudo apt update && sudo apt upgrade -y

# Build tools
sudo apt install -y build-essential cmake git wget unzip gdb

# Java (for Fast-DDS-Gen) + Core libraries
sudo apt install -y openjdk-17-jdk libboost-all-dev libasio-dev libtinyxml2-dev libssl-dev

# Rust (for Zenoh)
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
source "$HOME/.cargo/env"
```

## Phase 1: Workspace Structure

```bash
cd ~/ws

# Create standard directories
mkdir -p libs src install build
```

## Phase 2: Download External Libraries

```bash
cd ~/ws/libs

git clone https://github.com/foonathan/memory.git -b v0.7-3
git clone https://github.com/eProsima/Fast-CDR.git -b v2.2.5
git clone https://github.com/eProsima/Fast-DDS.git -b v2.14.5
git clone --recursive https://github.com/eProsima/Fast-DDS-Gen.git -b v3.3.1
git clone https://github.com/COVESA/vsomeip.git -b 3.5.8
git clone https://github.com/eclipse-zenoh/zenoh-c.git -b 1.8.0
git clone https://github.com/eclipse-zenoh/zenoh-cpp.git -b 1.8.0
```

## Phase 3: Build & Install Libraries

```bash
export INSTALL_DIR=$HOME/ws/install
export JOBS=$(nproc)

# 1. Foonathan Memory
mkdir -p ~/ws/build/memory && cd ~/ws/build/memory
cmake ../../libs/memory \
    -DCMAKE_INSTALL_PREFIX=$INSTALL_DIR \
    -DFOONATHAN_MEMORY_BUILD_EXAMPLES=OFF \
    -DFOONATHAN_MEMORY_BUILD_TESTS=OFF
make -j$JOBS && make install

# 2. Fast CDR
mkdir -p ~/ws/build/fastcdr && cd ~/ws/build/fastcdr
cmake ../../libs/Fast-CDR \
    -DCMAKE_INSTALL_PREFIX=$INSTALL_DIR
make -j$JOBS && make install

# 3. Fast DDS
mkdir -p ~/ws/build/fastdds && cd ~/ws/build/fastdds
cmake ../../libs/Fast-DDS \
    -DCMAKE_INSTALL_PREFIX=$INSTALL_DIR \
    -DCMAKE_PREFIX_PATH=$INSTALL_DIR \
    -DTHIRDPARTY=ON
make -j$JOBS && make install

# 4. vSomeIP
mkdir -p ~/ws/build/vsomeip && cd ~/ws/build/vsomeip
cmake ../../libs/vsomeip \
    -DCMAKE_INSTALL_PREFIX=$INSTALL_DIR \
    -DENABLE_SIGNAL_SLOT=ON
make -j$JOBS && make install

# 5. Zenoh-C (requires Rust, takes ~10 min)
mkdir -p ~/ws/build/zenohc && cd ~/ws/build/zenohc
cmake ../../libs/zenoh-c \
    -DCMAKE_INSTALL_PREFIX=$INSTALL_DIR
cmake --build . --target install -j$JOBS

# 6. Zenoh-Cpp (header-only)
mkdir -p ~/ws/build/zenohcpp && cd ~/ws/build/zenohcpp
cmake ../../libs/zenoh-cpp \
    -DCMAKE_INSTALL_PREFIX=$INSTALL_DIR \
    -DZENOHCXX_ZENOHC=ON
cmake --build . --target install

# 7. Fast-DDS-Gen
echo ">>> [7/7] Fast-DDS-Gen..."
cd ~/ws/libs/Fast-DDS-Gen
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
chmod +x gradlew && ./gradlew assemble > /dev/null
mkdir -p $INSTALL_DIR/share/java
find build/libs -name "fastddsgen*.jar" -exec cp {} $INSTALL_DIR/share/java/fastddsgen.jar \;
echo -e '#!/bin/bash\njava -jar '$INSTALL_DIR'/share/java/fastddsgen.jar "$@"' > $INSTALL_DIR/bin/fastddsgen
chmod +x $INSTALL_DIR/bin/fastddsgen

```

## Phase 4: Environment Setup Script

Create `~/ws/setup.sh`:

```bash
cat << 'EOF' > ~/ws/setup.sh
#!/bin/bash
# Pin the workspace root to ~/ws regardless of where this script is sourced from.
export WS_ROOT="$HOME/ws"
export INSTALL_DIR="$WS_ROOT/install"

export LD_LIBRARY_PATH="$INSTALL_DIR/lib:$LD_LIBRARY_PATH"
export PATH="$INSTALL_DIR/bin:$PATH"
export CMAKE_PREFIX_PATH="$INSTALL_DIR:$CMAKE_PREFIX_PATH"
export PKG_CONFIG_PATH="$INSTALL_DIR/lib/pkgconfig:$PKG_CONFIG_PATH"

echo "Gateway Isolated Env Loaded! (Prefix: $INSTALL_DIR)"
EOF

chmod +x ~/ws/setup.sh
```

## Phase 5: Clone Source Code

```bash
cd ~/ws/src
git clone https://github.com/ssuuhh00/multi-protocol-gateway.git

cd ~/ws/src/multi-protocol-gateway
git submodule update --init --recursive
```

## Phase 6: Build Gateway

```bash
# 1. Generate ROS 2 message sources
source ~/ws/setup.sh && ~/ws/src/multi-protocol-gateway/ros2/scripts/build_from_idls.sh

# 2. Generate type registry header
source ~/ws/setup.sh && ~/ws/src/multi-protocol-gateway/ros2/scripts/update_with_config.sh

# 3. Build gateway
source ~/ws/setup.sh && rm -rf ~/ws/build/gateway && mkdir -p ~/ws/build/gateway && cd ~/ws/build/gateway && cmake ../../src/multi-protocol-gateway && make -j$(nproc)
```

## Phase 7: Run

The gateway has no compile-time defaults — every config path must come from
environment variables. `VSOMEIP_*` is only required when the mapping uses
SOMEIP.

```bash
source ~/ws/setup.sh

export GATEWAY_MAPPING_PATH=~/ws/src/multi-protocol-gateway/examples/mappings/all_directions.json
export ZENOH_CONFIG_PATH=~/ws/src/multi-protocol-gateway/examples/configs/gateway_zenoh.json5
export VSOMEIP_CONFIG_PATH=~/ws/src/multi-protocol-gateway/examples/configs/gateway_vsomeip.json
export VSOMEIP_APP_NAME=gateway_app

~/ws/build/gateway/multi_protocol_gateway
```

See [`examples/README.md`](../examples/README.md) for the two-terminal demo
walkthrough.