# Encrypted ZIP Kafka JSON Bridge

Production-oriented C++17 service for RHEL 8.2:

```text
Kafka binary payload
       |
       v
KafkaReader thread
       |
       v
bounded ThreadQueue<vector<uint8_t>>
       |
       v
DataProcessor thread
       |
       +--> OpenSSL EVP AES-256-CBC decrypt (memory only)
       |
       +--> libzip open/decompress ZIP (memory only)
       |
       +--> RapidJSON ParseInsitu + PrettyWriter
       |
       v
stdout
```

## Important encryption contract

This implementation expects each Kafka message to be **raw AES-256-CBC ciphertext**. The decrypted bytes must be a complete ZIP archive.

Configuration is supplied through:

- `AES256_KEY_HEX`: exactly 64 hexadecimal characters = 32-byte AES-256 key.
- `AES256_IV_HEX`: exactly 32 hexadecimal characters = 16-byte CBC IV.

OpenSSL's EVP implementation performs PKCS#7-compatible CBC padding handling by default.

If the producer uses a different envelope (for example `IV || ciphertext`, a salt/header, or authenticated AES-GCM), the `Decryptor` contract must be changed accordingly. CBC alone does not provide authenticity; use an authenticated encryption format such as AES-GCM for new designs where the protocol can be changed.

## ZIP contract

The decrypted archive must contain at least one regular file ending in `.json`. The first matching JSON entry is extracted entirely in memory.

Default limits:

- Kafka message: 64 MiB
- decrypted ZIP: 64 MiB
- JSON entry: 16 MiB
- queue capacity: 128 messages

These limits are configurable and protect the process from unbounded memory use.

## Runtime configuration

```bash
export KAFKA_BROKERS="192.168.1.9:9092"
export KAFKA_TOPIC="position-update-blue"
export KAFKA_GROUP_ID="encrypted-json-bridge-test"
export KAFKA_AUTO_OFFSET_RESET="earliest"

export AES256_KEY_HEX="<64 hex characters>"
export AES256_IV_HEX="<32 hex characters>"

export QUEUE_CAPACITY="128"
export KAFKA_MAX_MESSAGE_BYTES="67108864"
export MAX_DECRYPTED_BYTES="67108864"
export MAX_JSON_BYTES="16777216"
```

Do not put the key or IV in source code or Git. Prefer a protected environment/service configuration or a secrets manager in production.

## Build

```bash
mkdir -p build
cd build
cmake ..
cmake --build . -j$(nproc)
```

If RapidJSON is installed in a non-standard location:

```bash
cmake .. -DRapidJSON_INCLUDE_DIR=/path/to/rapidjson/include
```

## Run

```bash
./kafka_encrypted_zip_bridge
```

Stop with `Ctrl+C` or send `SIGTERM`.

## Shutdown behavior

The POSIX signal handler only sets an atomic stop flag. It does not perform C++ I/O, allocation, locking, or Kafka/libzip/OpenSSL calls.

The Kafka thread exits its polling loop, closes the Kafka consumer, and closes the queue. Closing the queue wakes the processor thread. The processor drains already queued messages and then exits. The main thread joins both workers.
