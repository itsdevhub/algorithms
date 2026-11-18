#pragma once

#include <array>
#include <atomic>
#include <utility>

// Wait-free single-producer / single-consumer ring buffer.
//
// Contract: exactly ONE thread calls push(), exactly ONE thread calls pop().
// N must be a power of two. T must be default constructible and movable.
template <class T, std::size_t N>
class spsc_ring_buffer {
    static_assert(N >= 2 && (N & (N - 1)) == 0, "N must be a power of two");

public:
    // non-copyable, non-movable
    spsc_ring_buffer() = default;
    spsc_ring_buffer(const spsc_ring_buffer&) = delete;
    spsc_ring_buffer& operator=(const spsc_ring_buffer&) = delete;
    spsc_ring_buffer(spsc_ring_buffer&&) = delete;
    spsc_ring_buffer& operator=(spsc_ring_buffer&&) = delete;

    // Producer thread only. Returns false if the buffer is full.
    bool push(T value) {
        const auto head = _head.load(std::memory_order_relaxed); // I am the only writer
        const auto tail = _tail.load(std::memory_order_acquire); // see consumer's frees
        if (head - tail == N)
            return false; // full

        _buffer[head & (N - 1)] = std::move(value);
        _head.store(head + 1, std::memory_order_release); // publish the element

        return true;
    }

    // Consumer thread only. Returns false if the buffer is empty.
    bool pop(T& out) {
        const auto tail = _tail.load(std::memory_order_relaxed); // I am the only writer
        const auto head = _head.load(std::memory_order_acquire); // see producer's elements
        if (head == tail)
            return false; // empty

        out = std::move(_buffer[tail & (N - 1)]);
        _tail.store(tail + 1, std::memory_order_release); // free the slot

        return true;
    }

    static constexpr auto capacity() { return N; }

private:
    // Each index on its own cache line, so the two threads don''t fight (false sharing).
    alignas(64) std::atomic<std::size_t> _head{0}; // written by producer
    alignas(64) std::atomic<std::size_t> _tail{0}; // written by consumer
    alignas(64) std::array<T, N> _buffer{};
};
