cmake_minimum_required(VERSION 3.22)
project(redis_lzf C)

set(CMAKE_C_STANDARD 11)

add_library(redis_lzf SHARED lzf_c.c lzf_d.c)
