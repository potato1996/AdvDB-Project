cmake_minimum_required(VERSION 3.9)
project(AdvDB)

set(CMAKE_CXX_STANDARD 11)

set(SOURCE_FILES DataMng.cpp TransMng.cpp main.cpp)
add_executable(AdvDB ${SOURCE_FILES})
