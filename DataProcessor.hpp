#pragma once

#include "Decryptor.hpp"
#include "ThreadQueue.hpp"

#include <atomic>
#include <cstdint>
#include <string>
#include <thread>
#include <vector>

class DataProcessor final {
public:
    DataProcessor(ThreadQueue<std::vector<std::uint8_t>>& input,
                  Decryptor decryptor,
                  std::atomic_bool& stop_requested);
    ~DataProcessor();

    DataProcessor(const DataProcessor&) = delete;
    DataProcessor& operator=(const DataProcessor&) = delete;

    bool start(std::string& error);
    void join();

private:
    void run();
    bool parse_and_print(std::string json, std::string& error) const;

    ThreadQueue<std::vector<std::uint8_t>>& input_;
    Decryptor decryptor_;
    std::atomic_bool& stop_requested_;
    std::thread thread_;
};
