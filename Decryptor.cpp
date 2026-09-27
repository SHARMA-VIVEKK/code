#include "Decryptor.hpp"

#include <openssl/evp.h>
#include <zip.h>

#include <algorithm>
#include <cstring>
#include <limits>
#include <memory>
#include <sstream>

namespace {

struct EvpCipherCtxDeleter {
    void operator()(EVP_CIPHER_CTX* ctx) const noexcept
    {
        EVP_CIPHER_CTX_free(ctx);
    }
};

struct ZipSourceDeleter {
    void operator()(zip_source_t* source) const noexcept
    {
        if (source != nullptr) {
            zip_source_free(source);
        }
    }
};

struct ZipArchiveDeleter {
    void operator()(zip_t* archive) const noexcept
    {
        if (archive != nullptr) {
            int error = 0;
            zip_close(archive);
            (void)error;
        }
    }
};

bool safe_add(std::size_t a, std::size_t b, std::size_t limit, std::size_t& result)
{
    if (b > limit || a > limit - b) {
        return false;
    }
    result = a + b;
    return true;
}

} // namespace

Decryptor::Decryptor(DecryptorConfig config) : config_(std::move(config)) {}

bool Decryptor::decrypt(const std::uint8_t* ciphertext,
                        std::size_t ciphertext_size,
                        std::vector<std::uint8_t>& plaintext,
                        std::string& error) const
{
    if (ciphertext == nullptr || ciphertext_size == 0U) {
        error = "Encrypted Kafka payload is empty";
        return false;
    }

    if (config_.aes256_key.size() != 32U) {
        error = "AES-256 key must contain exactly 32 bytes";
        return false;
    }

    if (config_.iv.size() != 16U) {
        error = "AES-CBC IV must contain exactly 16 bytes";
        return false;
    }

    if (ciphertext_size % 16U != 0U) {
        error = "AES-CBC ciphertext size is not a multiple of the block size";
        return false;
    }

    if (ciphertext_size > config_.max_decrypted_bytes) {
        error = "Encrypted payload exceeds configured maximum size";
        return false;
    }

    std::unique_ptr<EVP_CIPHER_CTX, EvpCipherCtxDeleter> ctx(EVP_CIPHER_CTX_new());
    if (!ctx) {
        error = "EVP_CIPHER_CTX_new failed";
        return false;
    }

    if (EVP_DecryptInit_ex(ctx.get(), EVP_aes_256_cbc(), nullptr,
                           config_.aes256_key.data(), config_.iv.data()) != 1) {
        error = "EVP_DecryptInit_ex failed";
        return false;
    }

    plaintext.resize(ciphertext_size + EVP_MAX_BLOCK_LENGTH);
    int out_len = 0;
    int final_len = 0;

    if (EVP_DecryptUpdate(ctx.get(), plaintext.data(), &out_len,
                          ciphertext, static_cast<int>(ciphertext_size)) != 1) {
        error = "EVP_DecryptUpdate failed";
        plaintext.clear();
        return false;
    }

    if (EVP_DecryptFinal_ex(ctx.get(), plaintext.data() + out_len, &final_len) != 1) {
        error = "AES-256-CBC authentication/padding check failed";
        plaintext.clear();
        return false;
    }

    const std::size_t total = static_cast<std::size_t>(out_len + final_len);
    if (total > config_.max_decrypted_bytes) {
        error = "Decrypted ZIP exceeds configured maximum size";
        plaintext.clear();
        return false;
    }

    plaintext.resize(total);
    return true;
}

bool Decryptor::extract_json_from_zip(const std::vector<std::uint8_t>& zip_bytes,
                                      std::string& json,
                                      std::string& error) const
{
    if (zip_bytes.empty()) {
        error = "Decrypted ZIP archive is empty";
        return false;
    }

    if (zip_bytes.size() > static_cast<std::size_t>(std::numeric_limits<zip_uint64_t>::max())) {
        error = "ZIP archive is too large for libzip";
        return false;
    }

    zip_error_t zip_error;
    zip_error_init(&zip_error);

    zip_source_t* raw_source = zip_source_buffer_create(
        zip_bytes.data(), static_cast<zip_uint64_t>(zip_bytes.size()), 0, &zip_error);
    if (raw_source == nullptr) {
        std::ostringstream oss;
        oss << "Failed to create in-memory ZIP source: " << zip_error_strerror(&zip_error);
        error = oss.str();
        zip_error_fini(&zip_error);
        return false;
    }

    std::unique_ptr<zip_source_t, ZipSourceDeleter> source(raw_source);
    int open_error = 0;
    zip_t* raw_archive = zip_open_from_source(source.get(), ZIP_RDONLY, &open_error);
    if (raw_archive == nullptr) {
        std::ostringstream oss;
        oss << "Failed to open ZIP archive: " << zip_error_strerror(&zip_error);
        error = oss.str();
        zip_error_fini(&zip_error);
        return false;
    }

    // libzip owns the source after a successful zip_open_from_source().
    source.release();
    zip_error_fini(&zip_error);
    std::unique_ptr<zip_t, ZipArchiveDeleter> archive(raw_archive);

    const zip_int64_t count = zip_get_num_entries(archive.get(), ZIP_FL_UNCHANGED);
    if (count < 0) {
        error = "Failed to enumerate ZIP entries";
        return false;
    }

    if (count == 0) {
        error = "ZIP archive contains no entries";
        return false;
    }

    for (zip_uint64_t i = 0; i < static_cast<zip_uint64_t>(count); ++i) {
        zip_stat_t stat;
        zip_stat_init(&stat);
        if (zip_stat_index(archive.get(), i, ZIP_FL_UNCHANGED, &stat) != 0) {
            error = "Failed to inspect ZIP entry";
            return false;
        }

        if ((stat.valid & ZIP_STAT_NAME) == 0U || stat.name == nullptr) {
            continue;
        }

        const std::string name(stat.name);
        if (name.empty() || name.back() == '/') {
            continue; // directory
        }

        // This application expects a JSON document. Pick the first .json entry.
        if (name.size() < 5U || name.compare(name.size() - 5U, 5U, ".json") != 0) {
            continue;
        }

        if ((stat.valid & ZIP_STAT_SIZE) == 0U) {
            error = "JSON ZIP entry has no reported size";
            return false;
        }

        const zip_uint64_t entry_size = stat.size;
        if (entry_size > static_cast<zip_uint64_t>(config_.max_json_bytes)) {
            error = "JSON ZIP entry exceeds configured maximum size";
            return false;
        }

        if (entry_size > static_cast<zip_uint64_t>(std::numeric_limits<std::size_t>::max() - 1U)) {
            error = "JSON ZIP entry is too large for this platform";
            return false;
        }

        std::string candidate(static_cast<std::size_t>(entry_size), '\0');
        zip_file_t* file = zip_fopen_index(archive.get(), i, ZIP_FL_UNCHANGED);
        if (file == nullptr) {
            error = "Failed to open JSON entry in ZIP archive";
            return false;
        }

        std::size_t total_read = 0U;
        while (total_read < candidate.size()) {
            const zip_uint64_t remaining =
                static_cast<zip_uint64_t>(candidate.size() - total_read);
            const zip_uint64_t chunk = std::min<zip_uint64_t>(remaining, 1024U * 1024U);
            const zip_int64_t n = zip_fread(file, candidate.data() + total_read, chunk);
            if (n < 0) {
                zip_fclose(file);
                error = "Failed while decompressing JSON ZIP entry";
                return false;
            }
            if (n == 0) {
                break;
            }
            total_read += static_cast<std::size_t>(n);
        }

        if (zip_fclose(file) != 0) {
            error = "Failed to close JSON ZIP entry";
            return false;
        }

        if (total_read != candidate.size()) {
            error = "ZIP entry ended before its advertised size was read";
            return false;
        }

        json = std::move(candidate);
        return true;
    }

    error = "ZIP archive contains no .json file";
    return false;
}

bool Decryptor::decrypt_and_extract_json(const std::uint8_t* ciphertext,
                                         std::size_t ciphertext_size,
                                         std::string& json,
                                         std::string& error) const
{
    std::vector<std::uint8_t> zip_bytes;
    if (!decrypt(ciphertext, ciphertext_size, zip_bytes, error)) {
        return false;
    }

    return extract_json_from_zip(zip_bytes, json, error);
}
