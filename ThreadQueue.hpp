#pragma once

#include <condition_variable>
#include <cstddef>
#include <deque>
#include <mutex>
#include <optional>
#include <utility>

// Bounded, multi-producer/multi-consumer queue.
// close() prevents new pushes and wakes all waiting consumers.
template <typename T>
class ThreadQueue final {
public:
    explicit ThreadQueue(std::size_t capacity) : capacity_(capacity) {}

    ThreadQueue(const ThreadQueue&) = delete;
    ThreadQueue& operator=(const ThreadQueue&) = delete;

    bool push(T value)
    {
        std::unique_lock<std::mutex> lock(mutex_);
        not_full_cv_.wait(lock, [this] {
            return closed_ || queue_.size() < capacity_;
        });

        if (closed_) {
            return false;
        }

        queue_.emplace_back(std::move(value));
        lock.unlock();
        not_empty_cv_.notify_one();
        return true;
    }

    std::optional<T> pop()
    {
        std::unique_lock<std::mutex> lock(mutex_);
        not_empty_cv_.wait(lock, [this] {
            return closed_ || !queue_.empty();
        });

        if (queue_.empty()) {
            return std::nullopt;
        }

        T value = std::move(queue_.front());
        queue_.pop_front();
        lock.unlock();
        not_full_cv_.notify_one();
        return value;
    }

    void close()
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (closed_) {
            return;
        }
        closed_ = true;
        not_empty_cv_.notify_all();
        not_full_cv_.notify_all();
    }

    bool is_closed() const
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return closed_;
    }

private:
    const std::size_t capacity_;
    mutable std::mutex mutex_;
    std::condition_variable not_empty_cv_;
    std::condition_variable not_full_cv_;
    std::deque<T> queue_;
    bool closed_{false};
};
