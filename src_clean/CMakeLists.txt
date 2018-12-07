cmake_minimum_required(VERSION 2.8)
project(AdvDB)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=gnu++11")

add_executable(repcrec TransMng.cpp DataMng.cpp main.cpp)
