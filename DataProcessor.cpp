#include "DataProcessor.hpp"

#include <rapidjson/document.h>
#include <rapidjson/error/en.h>
#include <rapidjson/prettywriter.h>
#include <rapidjson/stringbuffer.h>

#include <iostream>
#include <utility>

DataProcessor::DataProcessor(ThreadQueue<std::vector<std::uint8_t>>& input,
                             Decryptor decryptor,
                             std::atomic_bool& stop_requested)
    : input_(input),
      decryptor_(std::move(decryptor)),
      stop_requested_(stop_requested)
{
}

DataProcessor::~DataProcessor()
{
    join();
}

bool DataProcessor::start(std::string& error)
{
    if (thread_.joinable()) {
        error = "Data processor already started";
        return false;
    }

    thread_ = std::thread(&DataProcessor::run, this);
    return true;
}

bool DataProcessor::parse_and_print(std::string json, std::string& error) const
{
    // RapidJSON in-situ parsing modifies the input buffer and therefore requires
    // a mutable, null-terminated string. std::string::data() is writable in C++17.
    rapidjson::Document document;
    document.ParseInsitu(json.data());

    if (document.HasParseError()) {
        error = std::string("JSON parse error: ") +
                rapidjson::GetParseError_En(document.GetParseError()) +
                " at offset " + std::to_string(document.GetErrorOffset());
        return false;
    }

    rapidjson::StringBuffer buffer;
    rapidjson::PrettyWriter<rapidjson::StringBuffer> writer(buffer);
    writer.SetIndent(' ', 2);
    if (!document.Accept(writer)) {
        error = "RapidJSON failed to serialize parsed document";
        return false;
    }

    std::cout << buffer.GetString() << '\n';
    std::cout.flush();
    return true;
}

void DataProcessor::run()
{
    for (;;) {
        std::optional<std::vector<std::uint8_t>> item = input_.pop();
        if (!item.has_value()) {
            // Queue is closed and drained. This is the normal shutdown path.
            break;
        }

        std::string json;
        std::string error;
        if (!decryptor_.decrypt_and_extract_json(item->data(), item->size(), json, error)) {
            std::cerr << "Payload processing failed: " << error << '\n';
            continue;
        }

        if (!parse_and_print(std::move(json), error)) {
            std::cerr << "JSON processing failed: " << error << '\n';
        }
    }

    stop_requested_.store(true, std::memory_order_relaxed);
}

void DataProcessor::join()
{
    if (thread_.joinable()) {
        thread_.join();
    }
}
