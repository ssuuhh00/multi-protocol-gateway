cmake_minimum_required(VERSION 3.16)
project(multi_protocol_gateway_examples LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
add_compile_options(-Wall -Wextra)

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif()

set(REPO_ROOT "${CMAKE_CURRENT_SOURCE_DIR}/..")
set(ROS2_MSG_DIR "${REPO_ROOT}/ros2/msgs")
set(EXAMPLES_SRC "${CMAKE_CURRENT_SOURCE_DIR}/src")

find_package(vsomeip3 REQUIRED)
find_package(fastrtps REQUIRED)
find_package(fastcdr REQUIRED)
find_package(zenohc REQUIRED)
find_package(zenohcxx REQUIRED)
find_package(Threads REQUIRED)

# Separate spdlog binary dir so it doesn't collide with a gateway build that
# lives in a different ${CMAKE_BINARY_DIR}.
add_subdirectory(${REPO_ROOT}/third_party/spdlog ${CMAKE_BINARY_DIR}/_spdlog)

# ros2_msgs_lib mirrors the root CMakeLists block intentionally — keep in sync
# when adding message types (USE_ macros in ros2/msgs/ros2_msg_config.h).

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
# See matching comment in root CMakeLists.txt for why libfmt is linked here.
target_link_libraries(ros2_msgs_lib PUBLIC fastrtps fastcdr fmt)

add_library(ext_nodes_demo STATIC
    ${EXAMPLES_SRC}/ext_nodes/ext_dds_pub.cpp
    ${EXAMPLES_SRC}/ext_nodes/ext_dds_sub.cpp
    ${EXAMPLES_SRC}/ext_nodes/ext_zenoh_pub.cpp
    ${EXAMPLES_SRC}/ext_nodes/ext_zenoh_sub.cpp
    ${EXAMPLES_SRC}/ext_nodes/ext_someip_server.cpp
    ${EXAMPLES_SRC}/ext_nodes/ext_someip_client.cpp
    # Compile gateway's logger_config.cpp here so gateway_demo gets
    # setup_global_logger() without linking the gateway binary.
    ${REPO_ROOT}/src/common/logger_config.cpp
)

target_include_directories(ext_nodes_demo PUBLIC
    ${EXAMPLES_SRC}
    ${REPO_ROOT}/src
    ${REPO_ROOT}/third_party
    ${ROS2_MSG_DIR}
)

target_compile_definitions(ext_nodes_demo PUBLIC
    ZENOHCXX_ZENOHC
)

target_compile_definitions(ext_nodes_demo PRIVATE
    DEFAULT_LOG_PATH="logs/demo.log"
)

target_link_libraries(ext_nodes_demo PUBLIC
    ros2_msgs_lib
    fastrtps
    fastcdr
    zenohc::lib
    zenohcxx::zenohc
    vsomeip3
    Threads::Threads
    spdlog::spdlog
)

add_executable(gateway_demo ${EXAMPLES_SRC}/demo.cpp)
target_link_libraries(gateway_demo PRIVATE ext_nodes_demo)
