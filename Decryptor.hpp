#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

struct DecryptorConfig {
    std::vector<std::uint8_t> aes256_key; // exactly 32 bytes
    std::vector<std::uint8_t> iv;         // exactly 16 bytes
    std::size_t max_decrypted_bytes{64U * 1024U * 1024U};
    std::size_t max_json_bytes{16U * 1024U * 1024U};
};

class Decryptor final {
public:
    explicit Decryptor(DecryptorConfig config);

    // Input is raw AES-256-CBC ciphertext. Output is the ZIP archive bytes.
    bool decrypt_and_extract_json(const std::uint8_t* ciphertext,
                                  std::size_t ciphertext_size,
                                  std::string& json,
                                  std::string& error) const;

private:
    bool decrypt(const std::uint8_t* ciphertext,
                 std::size_t ciphertext_size,
                 std::vector<std::uint8_t>& plaintext,
                 std::string& error) const;

    bool extract_json_from_zip(const std::vector<std::uint8_t>& zip_bytes,
                               std::string& json,
                               std::string& error) const;

    DecryptorConfig config_;
};
