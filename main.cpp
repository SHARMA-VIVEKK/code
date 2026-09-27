#include "DataProcessor.hpp"
#include "Decryptor.hpp"
#include "KafkaReader.hpp"
#include "ThreadQueue.hpp"

#include <csignal>
#include <cstdint>
#include <cstdlib>
#include <exception>
#include <iostream>
#include <limits>
#include <string>
#include <vector>

#include <chrono>
#include <thread>

namespace {
std::atomic_bool g_stop_requested{false};

void signal_handler(int signal_number)
{
    if (signal_number == SIGINT || signal_number == SIGTERM) {
        g_stop_requested.store(true, std::memory_order_relaxed);
    }
}

const char* getenv_or(const char* name, const char* fallback)
{
    const char* value = std::getenv(name);
    return (value != nullptr && *value != '\0') ? value : fallback;
}

bool parse_size(const char* value, std::size_t& result)
{
    if (value == nullptr || *value == '\0') {
        return false;
    }
    try {
        const unsigned long long parsed = std::stoull(value);
        if (parsed == 0ULL || parsed > std::numeric_limits<std::size_t>::max()) {
            return false;
        }
        result = static_cast<std::size_t>(parsed);
        return true;
    } catch (...) {
        return false;
    }
}

bool parse_hex(const std::string& hex, std::size_t expected_bytes,
               std::vector<std::uint8_t>& output, std::string& error)
{
    if (hex.size() != expected_bytes * 2U) {
        error = "Hex value must contain exactly " +
                std::to_string(expected_bytes * 2U) + " characters";
        return false;
    }

    auto hex_value = [](char c) -> int {
        if (c >= '0' && c <= '9') return c - '0';
        if (c >= 'a' && c <= 'f') return c - 'a' + 10;
        if (c >= 'A' && c <= 'F') return c - 'A' + 10;
        return -1;
    };

    output.resize(expected_bytes);
    for (std::size_t i = 0; i < expected_bytes; ++i) {
        const int high = hex_value(hex[2U * i]);
        const int low = hex_value(hex[2U * i + 1U]);
        if (high < 0 || low < 0) {
            error = "Hex value contains a non-hexadecimal character";
            output.clear();
            return false;
        }
        output[i] = static_cast<std::uint8_t>((high << 4) | low);
    }
    return true;
}

bool load_configuration(KafkaConfig& kafka,
                        DecryptorConfig& decryptor,
                        std::size_t& queue_capacity,
                        std::string& error)
{
    kafka.brokers = getenv_or("KAFKA_BROKERS", "localhost:9092");
    kafka.topic = getenv_or("KAFKA_TOPIC", "encrypted-json");
    kafka.group_id = getenv_or("KAFKA_GROUP_ID", "encrypted-json-bridge");
    kafka.auto_offset_reset = getenv_or("KAFKA_AUTO_OFFSET_RESET", "earliest");

    if (!parse_size(getenv_or("QUEUE_CAPACITY", "128"), queue_capacity)) {
        error = "QUEUE_CAPACITY must be a positive integer";
        return false;
    }

    if (!parse_size(getenv_or("KAFKA_MAX_MESSAGE_BYTES", "67108864"),
                    kafka.max_message_bytes)) {
        error = "KAFKA_MAX_MESSAGE_BYTES must be a positive integer";
        return false;
    }

    if (!parse_size(getenv_or("MAX_DECRYPTED_BYTES", "67108864"),
                    decryptor.max_decrypted_bytes)) {
        error = "MAX_DECRYPTED_BYTES must be a positive integer";
        return false;
    }

    if (!parse_size(getenv_or("MAX_JSON_BYTES", "16777216"),
                    decryptor.max_json_bytes)) {
        error = "MAX_JSON_BYTES must be a positive integer";
        return false;
    }

    const std::string key_hex = getenv_or("AES256_KEY_HEX", "");
    const std::string iv_hex = getenv_or("AES256_IV_HEX", "");
    if (!parse_hex(key_hex, 32U, decryptor.aes256_key, error)) {
        error = "AES256_KEY_HEX: " + error;
        return false;
    }
    if (!parse_hex(iv_hex, 16U, decryptor.iv, error)) {
        error = "AES256_IV_HEX: " + error;
        return false;
    }

    return true;
}
} // namespace

int main()
{
    std::signal(SIGINT, signal_handler);
    std::signal(SIGTERM, signal_handler);

    KafkaConfig kafka_config;
    DecryptorConfig decryptor_config;
    std::size_t queue_capacity = 0U;
    std::string error;

    if (!load_configuration(kafka_config, decryptor_config, queue_capacity, error)) {
        std::cerr << "Configuration error: " << error << '\n';
        return EXIT_FAILURE;
    }

    ThreadQueue<std::vector<std::uint8_t>> raw_payload_queue(queue_capacity);
    KafkaReader kafka_reader(kafka_config, raw_payload_queue, g_stop_requested);
    DataProcessor processor(raw_payload_queue, Decryptor(decryptor_config), g_stop_requested);

    if (!processor.start(error)) {
        std::cerr << "Failed to start data processor: " << error << '\n';
        raw_payload_queue.close();
        return EXIT_FAILURE;
    }

    if (!kafka_reader.start(error)) {
        std::cerr << "Failed to start Kafka reader: " << error << '\n';
        g_stop_requested.store(true, std::memory_order_relaxed);
        raw_payload_queue.close();
        processor.join();
        return EXIT_FAILURE;
    }

    std::cout << "Encrypted ZIP Kafka bridge started.\n"
              << "  Kafka brokers: " << kafka_config.brokers << '\n'
              << "  Kafka topic:   " << kafka_config.topic << '\n'
              << "  Consumer group:" << kafka_config.group_id << '\n';

    // Main thread waits until a signal is received or the processor encounters
    // a terminal queue shutdown. The signal handler only flips an atomic flag.
    while (!g_stop_requested.load(std::memory_order_relaxed)) {
        // Use a short sleep without introducing another worker thread.
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    raw_payload_queue.close();
    kafka_reader.join();
    processor.join();

    std::cout << "Encrypted ZIP Kafka bridge stopped cleanly.\n";
    return EXIT_SUCCESS;
}
