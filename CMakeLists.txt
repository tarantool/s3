cmake_minimum_required(VERSION 2.8 FATAL_ERROR)

# project(modulekit C)
project(s3 C CXX)

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE RelWithDebInfo)
endif()
set(CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}" ${CMAKE_MODULE_PATH})

# Find Tarantool and Lua dependecies
set(TARANTOOL_FIND_REQUIRED ON)
find_package(Tarantool)
include_directories(${TARANTOOL_INCLUDE_DIRS})

# Find MsgPuck llibrary
set(MsgPuck_FIND_REQUIRED ON)
#find_package(MsgPuck)
include_directories(${MSGPUCK_INCLUDE_DIRS})

# Find other dependecies

# Set CFLAGS
# set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS}")
set(CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} -Wall -Wextra")
# Set CXXFLAGS
# set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -Wall -Wextra")
execute_process ( COMMAND env BUILD=${CMAKE_BINARY_DIR}/third_party/libs3 make WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/third_party/libs3 )

# Build module
add_subdirectory(s3)
