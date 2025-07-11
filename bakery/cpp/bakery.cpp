#include <atomic>
#include <chrono>
#include <cstdint>
#include <iostream>
#include <thread>
#include <vector>

/*-----------------------------------------------------------
   Lamport's bakery algorithm
-----------------------------------------------------------*/

class bakery_mutex_naive
{
public:
    explicit bakery_mutex_naive(std::size_t n)
        : choosing(n, 0), number(n, 0)
    {        
    }

    std::size_t lock(std::size_t id)
    {
        auto my_ticket = announce_intent(id); // choose ticket
        wait_acquire(id);                     // wait for turn
        return my_ticket;                     // critical section may begin
    }

    void unlock(std::size_t id)
    {
        number[id] = 0; // release ticket
    }

private:
    std::size_t get_max_ticket() const
    {
        std::size_t max_ticket = 0;
        for (std::size_t t : number)
            max_ticket = std::max(max_ticket, t);
        return max_ticket;
    }

    std::size_t announce_intent(std::size_t id)
    {
        choosing[id] = 1;
        // pick ticket = 1 + max(number)
        std::size_t max_ticket = get_max_ticket() + 1;
        number[id] = max_ticket;
        choosing[id] = 0; // done choosing
        return max_ticket;
    }

    void wait_acquire(std::size_t id)
    {
        const std::size_t n = choosing.size();
        for (std::size_t j = 0; j < n; ++j) {
            if (j == id) continue;
            // wait while j is still choosing
            while (choosing[j])
                std::this_thread::yield();
            // wait while (ticket[j], j) < (ticket[i], i)
            while (true) {
                std::size_t tj = number[j];
                if (tj == 0) break;
                std::size_t ti = number[id];
                if (tj >  ti) break;
                if (tj == ti && j > id) break;
                std::this_thread::yield();
            }
        }
    }

    std::vector<std::uint64_t>  choosing;   // 0 = false, 1 = true
    std::vector<std::uint64_t>  number;     // ticket values
};

class bakery_mutex_atomic
{
public:
    explicit bakery_mutex_atomic(std::size_t n)
        : choosing(n), number(n)
    {
        for (auto& c : choosing)
            c.store(false, std::memory_order_relaxed);
        for (auto& t : number)
            t.store(0,     std::memory_order_relaxed);
    }

    std::size_t lock(std::size_t id)
    {
        auto my_ticket = announce_intent(id); // choose ticket
        wait_acquire(id);                     // wait for turn
        return my_ticket;                     // critical section may begin
    }

    void unlock(std::size_t id)
    {
        number[id].store(0, std::memory_order_seq_cst); // release ticket
    }

private:
    static inline void full_barrier()
    {
        std::atomic_thread_fence(std::memory_order_seq_cst);
    }

    std::size_t get_max_ticket() const
    {
        std::size_t max_ticket = 0;
        for (const auto& t : number)
            max_ticket = std::max(max_ticket, t.load(std::memory_order_seq_cst));
        return max_ticket;
    }

    std::size_t announce_intent(std::size_t id)
    {
        choosing[id].store(true, std::memory_order_seq_cst);
        // pick ticket = 1 + max(number)
        std::size_t my_ticket = get_max_ticket() + 1;
        number[id].store(my_ticket, std::memory_order_seq_cst);
        choosing[id].store(false, std::memory_order_seq_cst);  // done choosing
        full_barrier(); // make ordering global
        return my_ticket;
    }

    void wait_acquire(std::size_t id)
    {
        const std::size_t n = choosing.size();
        for (std::size_t j = 0; j < n; ++j) {
            if (j == id) continue;
            // wait while j is still choosing
            while (choosing[j].load(std::memory_order_seq_cst))
                std::this_thread::yield();
            // wait while (ticket[j], j) < (ticket[i], i)
            while (true) {
                std::size_t tj = number[j].load(std::memory_order_seq_cst);
                if (tj == 0) break;
                std::size_t ti = number[id].load(std::memory_order_seq_cst);
                if (tj >  ti) break;
                if (tj == ti && j > id) break;
                std::this_thread::yield();
            }
        }
    }

    std::vector<std::atomic<bool>>          choosing;  // true = thread is in doorway
    std::vector<std::atomic<std::size_t>>   number;    // ticket values
};

class bakery_mutex_bounded
{
public:
    explicit bakery_mutex_bounded(std::size_t n, std::size_t max_ticket)
        : choosing(n), number(n), max_ticket_allowed(max_ticket)
    {
        for (auto& c : choosing)
            c.store(false, std::memory_order_relaxed);
        for (auto& t : number)
            t.store(0,     std::memory_order_relaxed);
    }

    std::size_t lock(std::size_t id)
    {
        synchronized_wait_outside();          // assure ticket numbers bounded
        auto my_ticket = announce_intent(id); // choose ticket
        wait_acquire(id);                     // wait for turn
        return my_ticket;                     // critical section may begin
    }

    void unlock(std::size_t id)
    {
        number[id].store(0, std::memory_order_seq_cst); // release ticket
    }

private:
    static inline void full_barrier()
    {
        std::atomic_thread_fence(std::memory_order_seq_cst);
    }

    std::size_t get_max_ticket() const
    {
        std::size_t max_ticket = 0;
        for (const auto& t : number)
            max_ticket = std::max(max_ticket, t.load(std::memory_order_seq_cst));
        return max_ticket;
    }

    void synchronized_wait_outside()
    {
        auto max_ticket = get_max_ticket();
        if (max_ticket > max_ticket_allowed - choosing.size()) {
            while (get_max_ticket() > 0) {
                // wait until everybody unlocks and all tickets reset to 0
                std::this_thread::yield();
            }
        }
    }

    std::size_t announce_intent(std::size_t id)
    {
        choosing[id].store(true, std::memory_order_seq_cst);
        // pick ticket = 1 + max(number)
        std::size_t my_ticket = get_max_ticket() + 1;
        number[id].store(my_ticket, std::memory_order_seq_cst);
        choosing[id].store(false, std::memory_order_seq_cst);  // done choosing
        full_barrier(); // make ordering global
        return my_ticket;
    }

    void wait_acquire(std::size_t id)
    {
        const std::size_t n = choosing.size();
        for (std::size_t j = 0; j < n; ++j) {
            if (j == id) continue;
            // wait while j is still choosing
            while (choosing[j].load(std::memory_order_seq_cst))
                std::this_thread::yield();
            // wait while (ticket[j], j) < (ticket[i], i)
            while (true) {
                std::size_t tj = number[j].load(std::memory_order_seq_cst);
                if (tj == 0) break;
                std::size_t ti = number[id].load(std::memory_order_seq_cst);
                if (tj >  ti) break;
                if (tj == ti && j > id) break;
                std::this_thread::yield();
            }
        }
    }

    std::size_t                             max_ticket_allowed;
    std::vector<std::atomic<bool>>          choosing;  // true = thread is in doorway
    std::vector<std::atomic<std::size_t>>   number;    // ticket values
};

bool parse_positive(char const* s, std::size_t& out)
{
    char* end = nullptr;
    unsigned long long v = std::strtoull(s, &end, 10);
    if (end && *end == '\0' && v > 0) {
        out = static_cast<std::size_t>(v);
        return true;
    } else {
        return false;
    }
}

int main(int argc, char* argv[])
{
    constexpr std::size_t DEFAULT_THREADS = 16;
    constexpr std::size_t DEFAULT_LOOPS   = 1000*1000;

    std::size_t num_threads = DEFAULT_THREADS;
    std::size_t num_loops   = DEFAULT_LOOPS;

    if (argc >= 2 && !parse_positive(argv[1], num_threads)) {
        std::cerr << "Usage: " << argv[0] << " [n_threads] [iterations]\n";
        return 1;
    }
    if (argc >= 3 && !parse_positive(argv[2], num_loops)) {
        std::cerr << "Usage: " << argv[0] << " [n_threads] [iterations]\n";
        return 1;
    }

    bakery_mutex_naive mtx{num_threads};
    //bakery_mutex_atomic mtx{num_threads};
    //bakery_mutex_bounded mtx{num_threads, 1u << 16};

    std::uint64_t shared_counter = 0;
    std::size_t   max_ticket     = 0;

    auto worker = [&](std::size_t id)
    {
        for (std::size_t k = 0; k < num_loops; ++k) {
            std::size_t t = mtx.lock(id);
            ++shared_counter;                   // critical section
            max_ticket = std::max(max_ticket, t);
            mtx.unlock(id);
        }
    };

    std::vector<std::thread> threads;
    threads.reserve(num_threads);
    for (std::size_t i = 0; i < num_threads; ++i)
        threads.emplace_back(worker, i);

    for (auto& t : threads) t.join();

    auto expected = num_threads * num_loops;
    auto good = expected == shared_counter;
    std::cout << "Threads:    " << num_threads         << '\n'
              << "Iterations: " << num_loops           << '\n'
              << "Expected:   " << expected            << '\n'
              << "Observed:   " << shared_counter      << '\n'
              << "Max ticket: " << max_ticket          << '\n'
              << (good ? "Passed!" : "FAILED!")        << '\n'
              ;
}

// g++ -std=c++20 -O3 -flto -march=native -DNDEBUG -pthread bakery.cpp -o bakery
