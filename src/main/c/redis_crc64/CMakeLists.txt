cmake_minimum_required(VERSION 3.22)
project(redis_crc64 C)

set(CMAKE_C_STANDARD 11)

if(DEFINED ENV{JAVA_HOME})
    message("JAVA_HOME: $ENV{JAVA_HOME}")
else()
    message("NOT DEFINED JAVA_HOME VARIABLES")
endif()

include_directories("$ENV{JAVA_HOME}/include" "$ENV{JAVA_HOME}/include/linux")

add_library(redis_crc64 SHARED crccombine.c crcspeed.c crc64.c
        io_velo_rdb_RedisCrc.h)
