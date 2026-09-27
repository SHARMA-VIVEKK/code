#pragma once

#include "ThreadQueue.hpp"

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <string>
#include <thread>
#include <vector>

struct KafkaConfig {
    std::string brokers;
    std::string topic;
    std::string group_id;
    std::string auto_offset_reset{"earliest"};
    int consume_timeout_ms{250};
    std::size_t max_message_bytes{64U * 1024U * 1024U};
};

class KafkaReader final {
public:
    KafkaReader(KafkaConfig config,
                ThreadQueue<std::vector<std::uint8_t>>& output,
                std::atomic_bool& stop_requested);
    ~KafkaReader();

    KafkaReader(const KafkaReader&) = delete;
    KafkaReader& operator=(const KafkaReader&) = delete;

    bool start(std::string& error);
    void join();

private:
    bool configure_consumer(std::string& error);
    void run();

    KafkaConfig config_;
    ThreadQueue<std::vector<std::uint8_t>>& output_;
    std::atomic_bool& stop_requested_;
    void* consumer_{nullptr}; // owned RdKafka::KafkaConsumer; opaque in header
    std::thread thread_;
};
