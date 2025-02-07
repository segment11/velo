cmake_minimum_required(VERSION 3.22)
project(redis_crc64 C)

set(CMAKE_C_STANDARD 11)

#include_directories("${JAVA_HOME}/include" "${JAVA_HOME}/include/linux")
include_directories("/home/kerry/Downloads/zulu21/include" "/home/kerry/Downloads/zulu21/include/linux")

add_library(redis_crc64 SHARED crccombine.c crcspeed.c crc64.c
        io_velo_rdb_RedisCrc.h)
