cmake_minimum_required(VERSION 3.16)
project(multi_protocol_gateway)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
add_compile_options(-Wall -Wextra)

set(ROS2_MSG_DIR "${CMAKE_SOURCE_DIR}/ros2/msgs")

find_package(vsomeip3 REQUIRED)
find_package(fastrtps REQUIRED)
find_package(fastcdr REQUIRED)
find_package(zenohc REQUIRED)
find_package(zenohcxx REQUIRED)
find_package(Threads REQUIRED)
find_package(Python3 REQUIRED)

add_subdirectory(third_party/spdlog)

# ros2_msgs_lib: enabled messages come from USE_<MSG> macros in
# ros2/msgs/ros2_msg_config.h; transitive deps resolved via ros2_msg_dependencies.map.
# NOTE: examples/CMakeLists.txt mirrors this block — keep in sync when adding message types.

set(_MSG_CONFIG_FILE "${ROS2_MSG_DIR}/ros2_msg_config.h")
set(_MSG_DEPS_FILE "${ROS2_MSG_DIR}/ros2_msg_dependencies.map")

set(DDS_GEN_SOURCES "")
set(SELECTED_MACROS "")

if(EXISTS ${_MSG_CONFIG_FILE})
    file(READ "${_MSG_CONFIG_FILE}" _CONFIG_CONTENT)
    string(REPLACE "\n" ";" _CONFIG_LINES "${_CONFIG_CONTENT}")
    foreach(line ${_CONFIG_LINES})
        if(line MATCHES "^[ \t]*#define[ \t]+USE_([A-Z0-9_]+)")
            list(APPEND SELECTED_MACROS "${CMAKE_MATCH_1}")
        endif()
    endforeach()
    if(SELECTED_MACROS)
        message(STATUS "----------------------------------------------------------------------")
        message(STATUS "[ros2_msgs] Selected ROS 2 Messages for Compilation:")
        foreach(_m ${SELECTED_MACROS})
            message(STATUS "  * ${_m}")
        endforeach()
        message(STATUS "----------------------------------------------------------------------")
    else()
        message(WARNING "[ros2_msgs] No USE_ macros found in ros2_msg_config.h")
    endif()
else()
    message(FATAL_ERROR "[ros2_msgs] Config file not found: ${_MSG_CONFIG_FILE}")
endif()

if(EXISTS ${_MSG_DEPS_FILE})
    file(STRINGS "${_MSG_DEPS_FILE}" _DEP_LINES)
    foreach(line ${_DEP_LINES})
        string(REPLACE ": " ";" _LINE_DATA "${line}")
        list(GET _LINE_DATA 0 _MSG_PATH)
        list(GET _LINE_DATA 1 _ALL_REQS)

        string(REPLACE "/" ";" _PATH_PARTS "${_MSG_PATH}")
        list(GET _PATH_PARTS 0 _PKG)
        list(GET _PATH_PARTS 1 _SUBDIR)
        list(REVERSE _PATH_PARTS)
        list(GET _PATH_PARTS 0 _NAME)

        string(REGEX REPLACE "([a-z0-9])([A-Z])" "\\1_\\2" _NAME_SNAKE "${_NAME}")
        string(TOUPPER "${_PKG}_${_SUBDIR}_${_NAME_SNAKE}" _MSG_KEY)

        if(_MSG_KEY IN_LIST SELECTED_MACROS)
            separate_arguments(_REQ_LIST NATIVE_COMMAND "${_ALL_REQS}")
            foreach(_REQ ${_REQ_LIST})
                set(_B_PATH "${ROS2_MSG_DIR}/${_REQ}")
                list(APPEND DDS_GEN_SOURCES
                    "${_B_PATH}.cxx"
                    "${_B_PATH}PubSubTypes.cxx"
                    "${_B_PATH}TypeObject.cxx"
                )
            endforeach()
        endif()
    endforeach()
else()
    message(FATAL_ERROR "[ros2_msgs] Dependency map not found: ${_MSG_DEPS_FILE}")
endif()

if(NOT DDS_GEN_SOURCES)
    message(FATAL_ERROR "[ros2_msgs] No messages selected — check ros2_msg_config.h")
endif()

list(REMOVE_DUPLICATES DDS_GEN_SOURCES)

add_library(ros2_msgs_lib STATIC
    ${DDS_GEN_SOURCES}
    ${ROS2_MSG_DIR}/ros2_msg_data_converter.cpp
)
target_include_directories(ros2_msgs_lib PUBLIC ${ROS2_MSG_DIR})
# fmt is required because Fast DDS' generated TypeObject sources include fastrtps
# headers that emit fmt::vN::... references. On distros where fastrtps' CMake
# config doesn't transitively expose libfmt (e.g. Jetson Ubuntu 22.04 / libfmt8),
# the link fails with "undefined reference to fmt::vN::detail::error_handler::on_error".
target_link_libraries(ros2_msgs_lib PUBLIC fastrtps fastcdr fmt)

set(GATEWAY_SOURCES
    src/gateway/gateway_core.cpp
    src/common/logger_config.cpp
    src/common/qos_spec.cpp
    src/common/resource_tracker.cpp
    src/dds/dds_publisher.cpp
    src/dds/dds_subscriber.cpp
    src/someip/someip_client.cpp
    src/someip/someip_server.cpp
    src/zenoh/zenoh_publisher.cpp
    src/zenoh/zenoh_subscriber.cpp
    src/discovery/discovery_manager.cpp
    src/discovery/discovery_mapping.cpp
)

add_executable(multi_protocol_gateway
    src/main.cpp
    ${GATEWAY_SOURCES}
)

target_include_directories(multi_protocol_gateway PUBLIC
    ${CMAKE_SOURCE_DIR}/src
    ${CMAKE_SOURCE_DIR}/third_party
    ${ROS2_MSG_DIR}
)

# Runtime config paths are NOT baked in: user must supply GATEWAY_MAPPING_PATH,
# ZENOH_CONFIG_PATH, and (if a mapping uses SOMEIP) VSOMEIP_CONFIG_PATH +
# VSOMEIP_APP_NAME via env. GatewayCore throws on missing required vars.
target_compile_definitions(multi_protocol_gateway PRIVATE
    ZENOHCXX_ZENOHC
    DEFAULT_LOG_PATH="logs/gateway.log"
)

# [6-1b] Zenoh unstable API.
#
# The installed libzenohc is built with ZENOHC_BUILD_WITH_UNSTABLE_API=ON and
# exports the ze_advanced_* entry points, but that says nothing about how OUR
# translation units see the headers: zenoh_commons.h wraps the whole Advanced
# Pub/Sub surface -- the option structs included -- in
# `#if defined(Z_FEATURE_UNSTABLE_API)`, and zenoh-cpp's ext/ wrappers do the
# same. Without this define the types do not exist for us even though the
# symbols are in the .so, so the QoS policies that ride on AdvancedPublisher /
# AdvancedSubscriber (history, durability, sample recovery) cannot be reached.
#
# zenohcConfig.cmake sets ZENOHC_BUILD_WITH_UNSTABLE_API as a plain variable
# rather than an INTERFACE compile definition, so it does not propagate through
# the imported target. Check it and fail loudly rather than silently building a
# gateway whose Zenoh leg is quietly less capable than the library it links.
if(NOT ZENOHC_BUILD_WITH_UNSTABLE_API)
    message(FATAL_ERROR
        "zenohc at ${CMAKE_PREFIX_PATH} was built without the unstable API. "
        "Rebuild it with -DZENOHC_BUILD_WITH_UNSTABLE_API=ON; the gateway's "
        "Zenoh QoS mapping depends on the Advanced Pub/Sub extension.")
endif()

target_compile_definitions(multi_protocol_gateway PRIVATE Z_FEATURE_UNSTABLE_API)

target_link_libraries(multi_protocol_gateway
    PRIVATE
    ros2_msgs_lib
    fastrtps
    fastcdr
    vsomeip3
    zenohc::lib
    zenohcxx::zenohc
    Threads::Threads
    spdlog::spdlog
)

add_custom_command(TARGET multi_protocol_gateway POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E echo ""
    COMMAND ${CMAKE_COMMAND} -E echo "----------------------------------------------------------------"
    COMMAND ${CMAKE_COMMAND} -E echo " Build Complete: multi_protocol_gateway"
    COMMAND ${CMAKE_COMMAND} -E echo "----------------------------------------------------------------"
    COMMAND ${CMAKE_COMMAND} -E echo " [Environment Variables]"
    COMMAND ${CMAKE_COMMAND} -E echo "  $ export GATEWAY_MAPPING_PATH=\"/path/to/mappings.json\""
    COMMAND ${CMAKE_COMMAND} -E echo "  $ export ZENOH_CONFIG_PATH=\"/path/to/zenoh_config.json5\""
    COMMAND ${CMAKE_COMMAND} -E echo "  $ export VSOMEIP_CONFIG_PATH=\"/path/to/vsomeip.json\""
    COMMAND ${CMAKE_COMMAND} -E echo "  $ export VSOMEIP_APP_NAME=\"gateway_app\""
    COMMAND ${CMAKE_COMMAND} -E echo "  $ export LOG_PATH=\"/path/to/logs/gateway.log\""
    COMMAND ${CMAKE_COMMAND} -E echo ""
    COMMAND ${CMAKE_COMMAND} -E echo " [Run]"
    COMMAND ${CMAKE_COMMAND} -E echo "  multi_protocol_gateway     # no argv — all paths come from env"
    COMMAND ${CMAKE_COMMAND} -E echo "----------------------------------------------------------------"
    COMMAND ${CMAKE_COMMAND} -E echo ""
    VERBATIM
)
