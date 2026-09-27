#include "KafkaReader.hpp"

#include <rdkafkacpp.h>

#include <iostream>
#include <memory>
#include <utility>

KafkaReader::KafkaReader(KafkaConfig config,
                         ThreadQueue<std::vector<std::uint8_t>>& output,
                         std::atomic_bool& stop_requested)
    : config_(std::move(config)),
      output_(output),
      stop_requested_(stop_requested)
{
}

KafkaReader::~KafkaReader()
{
    join();
    delete static_cast<RdKafka::KafkaConsumer*>(consumer_);
    consumer_ = nullptr;
}

bool KafkaReader::configure_consumer(std::string& error)
{
    std::string errstr;
    std::unique_ptr<RdKafka::Conf> conf(
        RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));
    if (!conf) {
        error = "Failed to allocate Kafka global configuration";
        return false;
    }

    auto set = [&](const char* key, const std::string& value) -> bool {
        if (conf->set(key, value, errstr) != RdKafka::Conf::CONF_OK) {
            error = std::string("Kafka config '") + key + "' failed: " + errstr;
            return false;
        }
        return true;
    };

    if (config_.brokers.empty() || config_.topic.empty() || config_.group_id.empty()) {
        error = "Kafka brokers, topic and group.id must not be empty";
        return false;
    }

    if (!set("bootstrap.servers", config_.brokers) ||
        !set("group.id", config_.group_id) ||
        !set("enable.auto.commit", "true") ||
        !set("auto.offset.reset", config_.auto_offset_reset) ||
        !set("enable.partition.eof", "false")) {
        return false;
    }

    // Allow the consumer to fetch a message up to the application's bound.
    // librdkafka expects these byte-valued properties as strings.
    const std::string max_bytes = std::to_string(config_.max_message_bytes);
    if (!set("max.partition.fetch.bytes", max_bytes) ||
        !set("fetch.max.bytes", max_bytes)) {
        return false;
    }

    RdKafka::KafkaConsumer* consumer = RdKafka::KafkaConsumer::create(conf.get(), errstr);
    if (!consumer) {
        error = "KafkaConsumer creation failed: " + errstr;
        return false;
    }
    consumer_ = consumer;

    const std::vector<std::string> topics{config_.topic};
    const RdKafka::ErrorCode rc = consumer->subscribe(topics);
    if (rc != RdKafka::ERR_NO_ERROR) {
        error = "Kafka subscribe failed: " + RdKafka::err2str(rc);
        delete consumer;
        consumer_ = nullptr;
        return false;
    }

    return true;
}

bool KafkaReader::start(std::string& error)
{
    if (thread_.joinable()) {
        error = "Kafka reader already started";
        return false;
    }

    if (!configure_consumer(error)) {
        return false;
    }

    thread_ = std::thread(&KafkaReader::run, this);
    return true;
}

void KafkaReader::run()
{
    auto* consumer = static_cast<RdKafka::KafkaConsumer*>(consumer_);

    while (!stop_requested_.load(std::memory_order_relaxed)) {
        std::unique_ptr<RdKafka::Message> message(
            consumer->consume(config_.consume_timeout_ms));

        if (!message) {
            continue;
        }

        if (message->err() == RdKafka::ERR_NO_ERROR) {
            const auto length = message->len();
            if (length == 0U || message->payload() == nullptr) {
                std::cerr << "Kafka message has empty payload; ignoring\n";
                continue;
            }

            if (length > config_.max_message_bytes) {
                std::cerr << "Kafka message exceeds configured maximum size; ignoring\n";
                continue;
            }

            const auto* begin = static_cast<const std::uint8_t*>(message->payload());
            std::vector<std::uint8_t> payload(begin, begin + length);

            if (!output_.push(std::move(payload))) {
                break; // queue closed during shutdown
            }
        } else if (message->err() == RdKafka::ERR__TIMED_OUT ||
                   message->err() == RdKafka::ERR__PARTITION_EOF) {
            continue;
        } else {
            std::cerr << "Kafka consume error: " << message->errstr() << '\n';
        }
    }

    consumer->close();
    output_.close();
}

void KafkaReader::join()
{
    if (thread_.joinable()) {
        thread_.join();
    }
}
