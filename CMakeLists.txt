cmake_minimum_required(VERSION 3.18)
project(kafka_encrypted_zip_bridge LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

add_compile_options(-Wall -Wextra -Wpedantic -Wconversion -Wsign-conversion)

find_package(Threads REQUIRED)
find_package(OpenSSL REQUIRED COMPONENTS Crypto)
find_package(PkgConfig REQUIRED)

pkg_check_modules(RDKAFKA REQUIRED IMPORTED_TARGET rdkafka++)
pkg_check_modules(LIBZIP REQUIRED IMPORTED_TARGET libzip)
pkg_check_modules(RAPIDJSON REQUIRED IMPORTED_TARGET RapidJSON)

add_executable(kafka_encrypted_zip_bridge
    src/main.cpp
    src/Decryptor.cpp
    src/KafkaReader.cpp
    src/DataProcessor.cpp
)

target_include_directories(kafka_encrypted_zip_bridge PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

target_link_libraries(kafka_encrypted_zip_bridge PRIVATE
    PkgConfig::RDKAFKA
    PkgConfig::LIBZIP
    OpenSSL::Crypto
    Threads::Threads
)

# RapidJSON is header-only. Some RHEL/EPEL installations expose it through
# pkg-config; if not, set RapidJSON_INCLUDE_DIR when configuring CMake.
if (RAPIDJSON_INCLUDE_DIRS)
    target_include_directories(kafka_encrypted_zip_bridge PRIVATE ${RAPIDJSON_INCLUDE_DIRS})
else()
    find_path(RapidJSON_INCLUDE_DIR rapidjson/document.h
        PATHS /usr/include /usr/local/include
        REQUIRED)
    target_include_directories(kafka_encrypted_zip_bridge PRIVATE ${RapidJSON_INCLUDE_DIR})
endif()

install(TARGETS kafka_encrypted_zip_bridge RUNTIME DESTINATION bin)
