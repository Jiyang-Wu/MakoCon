// Build: g++ bench.cpp -O3 -std=gnu++17 -lhiredis -lpthread -o bench

// redis-server --port 6379 --io-threads 8 --io-threads-do-reads yes
// redis-cli -h 127.0.0.1 -p 6379 SHUTDOWN

// Basically:
//      Spawns a (#number) of clients on their own connections,
//      for each client during a given (#time interval)
//      constantly execute GET/SET requests of different (#sizes) of payloads
//      and summarize the final results into a csv table

#include <hiredis/hiredis.h>
#include <algorithm>
#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <iostream>
#include <numeric>
#include <random>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

using Clock = std::chrono::steady_clock;
using ns = std::chrono::nanoseconds;

struct Target {
    std::string name{"redis"};
    std::string host{"127.0.0.1"};
    int port{6379};
};

struct Args {
    Target t1;                         // redis by default
    Target t2{"mako", "127.0.0.1", 6380};
    uint64_t keys{1000000};
    int warmup_sec{10};
    std::vector<int> clients{16, 32, 64};
    std::vector<int> values{64, 256, 1024};
    int duration{30};
    std::string out_csv{"results.csv"};
};

struct BenchRow {
    Target t;
    std::string op;       // "get" or "set"
    int clients;
    int value_size;
    int seconds;
    uint64_t ops;         // total ops completed
    double ops_sec;       // ops/second
    double p50_us;
    double p95_us;
    double p99_us;
};

struct CsvWriter {
    explicit CsvWriter(const std::string &path)
        : ofs(path) {
        if (!ofs) {
            throw std::runtime_error("Cannot open CSV: " + path);
        }
    }

    void write_header() {
        ofs << "server,host,port,op,clients,value_size,seconds,ops,ops_per_sec,p50_us,p95_us,p99_us\n";
    }

    void write(const std::vector<BenchRow> &rows) {
        for (const auto &r : rows) {
            ofs << r.t.name << ','
                << r.t.host << ','
                << r.t.port << ','
                << r.op << ','
                << r.clients << ','
                << r.value_size << ','
                << r.seconds << ','
                << r.ops << ','
                << std::fixed << r.ops_sec << ','
                << r.p50_us << ','
                << r.p95_us << ','
                << r.p99_us << '\n';
        }

        ofs.flush();
    }

private:
    std::ofstream ofs;
};

static std::atomic<bool> g_stop{false};

static redisContext *connect_retry(const std::string &host, int port, int tries = 20, int ms = 200) {
    for (int i = 0; i < tries; i++) {
        // attempts to connect to the redis server at the designated port with 2 sec timeout (if no handshake in 2s, attempt fails)
        timeval tv{2, 0};
        redisContext *c = redisConnectWithTimeout(host.c_str(), port, tv);
        if (c != nullptr) {
            if (!c->err) {
                return c;
            } else {
                redisFree(c);
            }
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(ms));
    }

    return nullptr;
}

// connects to the host in Target and pings to ensure successful connection
static void ping_or_exit(const Target &t) {
    redisContext *c = connect_retry(t.host, t.port);
    if (c == nullptr) {
        throw std::runtime_error("Connect failed: " + t.host + ":" + std::to_string(t.port));
    }

    redisReply *r = (redisReply *)redisCommand(c, "PING");
    if (r == nullptr) {
        redisFree(c);
        throw std::runtime_error("PING failed: " + t.host + ":" + std::to_string(t.port));
    }
    freeReplyObject(r);
    redisFree(c);
}

// preloads keys number of 'XXXXX' values of vsize into the databases
static void preload(const Target &t, uint64_t keys, int vsize) {
    redisContext *c = connect_retry(t.host, t.port);

    if (c == nullptr) {
        throw std::runtime_error("Preload connect failed: " + t.host + ":" + std::to_string(t.port));
    }
    std::string val(vsize, 'X');
    for (uint64_t k = 1; k <= keys; k++) {
        std::string key = "key:" + std::to_string(k);
        redisReply *r = (redisReply *)redisCommand(c, "SET %s %b", key.c_str(), val.data(), (size_t)vsize);

        if (r != nullptr) {
            freeReplyObject(r);
        }
    }
    redisFree(c);
}

// starts up 16 threads and constantly executes random-generated GET requests over "sec" seconds
static void warmup(const Target &t, uint64_t keys, int sec) {
    std::vector<std::thread> th;
    int clients = 16;

    for (int i = 0; i < clients; i++) {
        th.emplace_back([&, i]() {
            redisContext *c = connect_retry(t.host, t.port);
            if (c == nullptr) {
                return;
            }

            std::mt19937_64 rng(0xBEEF + i);
            std::uniform_int_distribution<uint64_t> d(1, keys);
            auto end = Clock::now() + std::chrono::seconds(sec);

            while (Clock::now() < end) {
                std::string key = "key:" + std::to_string(d(rng));
                redisReply *r = (redisReply *)redisCommand(c, "GET %s", key.c_str());

                if (r != nullptr) {
                    freeReplyObject(r);
                }
            }

            redisFree(c);
        });
    }

    for (auto &x : th) {
        x.join();
    }
}

// caluculates the percentile of the given list of sorted latencies
static inline double pct_us(std::vector<uint64_t> &ns_sorted, double p) {
    if (ns_sorted.empty()) {
        return 0.0;
    }

    size_t idx = static_cast<size_t>(p * (ns_sorted.size() - 1));
    double ns_val = static_cast<double>(ns_sorted[idx]);
    return ns_val / 1000.0;
}

struct WorkerCfg {
    Target t;
    std::string op;
    uint64_t keys;
    int value_size;
    int seconds;
    uint64_t seed;
};

struct Stats {
    uint64_t ops{0};
    std::vector<uint64_t> lats;
};

// spawns "clients" number of threads, each performing:
//      Constantly executing get/set requests over the given time interval
//      Stores results (number of operations + latencies(up to 2000 results)) in its corresponding entry in the arrayList "per"
// Count the total number of successful operations and sorted latencies of each operation
static Stats run_workers_no_pipeline(const WorkerCfg &cfg, int clients) {
    std::vector<std::thread> th;
    std::vector<Stats> per(clients);
    std::atomic<bool> start{false};

    for (int i = 0; i < clients; i++) {
        th.emplace_back([&, i]() {
            redisContext *c = connect_retry(cfg.t.host, cfg.t.port);

            if (c == nullptr) {
                return;
            }

            std::string val(cfg.value_size, 'Y');
            std::mt19937_64 rng(cfg.seed + i * 1337ULL);
            std::uniform_int_distribution<uint64_t> d(1, cfg.keys);
            per[i].lats.reserve(2000);

            while (!start.load()) {
                std::this_thread::yield();
            }

            auto end = Clock::now() + std::chrono::seconds(cfg.seconds);

            while (Clock::now() < end && !g_stop.load()) {
                std::string key = "key:" + std::to_string(d(rng));
                auto t0 = Clock::now();
                redisReply *r = nullptr;

                if (cfg.op == "get") {
                    r = (redisReply *)redisCommand(c, "GET %s", key.c_str());
                } else {
                    r = (redisReply *)redisCommand(c, "SET %s %b", key.c_str(), val.data(), (size_t)cfg.value_size);
                }
                auto t1 = Clock::now();
                if (r != nullptr) {
                    freeReplyObject(r);
                    per[i].ops++;
                }

                if (per[i].lats.size() < per[i].lats.capacity()) {
                    per[i].lats.push_back(std::chrono::duration_cast<ns>(t1 - t0).count());
                }
            }
            redisFree(c);
        });
    }

    start.store(true);
    for (auto &x : th) {
        x.join();
    }
    Stats out;
    for (auto &s : per) {
        out.ops += s.ops;
    }
    std::vector<uint64_t> all;
    size_t n = 0;
    for (auto &s : per) {
        n += s.lats.size();
    }

    all.reserve(n);

    for (auto &s : per) {
        all.insert(all.end(), s.lats.begin(), s.lats.end());
    }

    std::sort(all.begin(), all.end());
    out.lats = std::move(all);

    return out;
}

struct BenchEngine {
    void prepare(const Args &a) {
        ping_or_exit(a.t1);
        ping_or_exit(a.t2);

        int max_v = *std::max_element(a.values.begin(), a.values.end());

        // Preload both with largest value size
        preload(a.t1, a.keys, max_v);
        preload(a.t2, a.keys, max_v);

        // Warmup
        warmup(a.t1, a.keys, a.warmup_sec);
        warmup(a.t2, a.keys, a.warmup_sec);
    }

    // runs all operations as scheduled and record the results in a matrix
    std::vector<BenchRow> run_matrix(const Args &a) {
        std::vector<BenchRow> rows;
        rows.reserve(a.values.size() * a.clients.size() * 2 /*get+set*/ * 2 /*targets*/);

        for (int v : a.values) {
            for (int c : a.clients) {
                std::cout << "[RUN] clients=" << c << " (GET+SET for " << a.t1.name << " & " << a.t2.name << ")\n" << std::flush;
                {
                    WorkerCfg cfg1{a.t1, "get", a.keys, v, a.duration, 0xC0FFEE};
                    Stats s1 = run_workers_no_pipeline(cfg1, c);

                    BenchRow r1;
                    r1.t = a.t1;
                    r1.op = "get";
                    r1.clients = c;
                    r1.value_size = v;
                    r1.seconds = a.duration;
                    r1.ops = s1.ops;
                    r1.ops_sec = static_cast<double>(s1.ops) / static_cast<double>(a.duration);
                    r1.p50_us = pct_us(s1.lats, 0.50);
                    r1.p95_us = pct_us(s1.lats, 0.95);
                    r1.p99_us = pct_us(s1.lats, 0.99);
                    rows.push_back(std::move(r1));

                    WorkerCfg cfg2{a.t2, "get", a.keys, v, a.duration, 0xDEADBEEF};
                    Stats s2 = run_workers_no_pipeline(cfg2, c);

                    BenchRow r2;
                    r2.t = a.t2;
                    r2.op = "get";
                    r2.clients = c;
                    r2.value_size = v;
                    r2.seconds = a.duration;
                    r2.ops = s2.ops;
                    r2.ops_sec = static_cast<double>(s2.ops) / static_cast<double>(a.duration);
                    r2.p50_us = pct_us(s2.lats, 0.50);
                    r2.p95_us = pct_us(s2.lats, 0.95);
                    r2.p99_us = pct_us(s2.lats, 0.99);
                    rows.push_back(std::move(r2));
                }

                {
                    WorkerCfg cfg1{a.t1, "set", a.keys, v, a.duration, 0xC0FFEE ^ 0x1234};
                    Stats s1 = run_workers_no_pipeline(cfg1, c);

                    BenchRow r1;
                    r1.t = a.t1;
                    r1.op = "set";
                    r1.clients = c;
                    r1.value_size = v;
                    r1.seconds = a.duration;
                    r1.ops = s1.ops;
                    r1.ops_sec = static_cast<double>(s1.ops) / static_cast<double>(a.duration);
                    r1.p50_us = pct_us(s1.lats, 0.50);
                    r1.p95_us = pct_us(s1.lats, 0.95);
                    r1.p99_us = pct_us(s1.lats, 0.99);
                    rows.push_back(std::move(r1));

                    WorkerCfg cfg2{a.t2, "set", a.keys, v, a.duration, 0xDEADBEEF ^ 0x1234};
                    Stats s2 = run_workers_no_pipeline(cfg2, c);

                    BenchRow r2;
                    r2.t = a.t2;
                    r2.op = "set";
                    r2.clients = c;
                    r2.value_size = v;
                    r2.seconds = a.duration;
                    r2.ops = s2.ops;
                    r2.ops_sec = static_cast<double>(s2.ops) / static_cast<double>(a.duration);
                    r2.p50_us = pct_us(s2.lats, 0.50);
                    r2.p95_us = pct_us(s2.lats, 0.95);
                    r2.p99_us = pct_us(s2.lats, 0.99);
                    rows.push_back(std::move(r2));
                }
            }
        }

        return rows;
    }
};

static void on_sigint(int) {
    g_stop.store(true);
}

static void usage(const char *p) {
    std::cerr
        << "Usage: " << p << " [options]\n"
        << "  --name1 N  --host1 H --port1 P\n"
        << "  --name2 N  --host2 H --port2 P\n"
        << "  --keys N            (default 1000000)\n"
        << "  --warmup-sec S      (default 10)\n"
        << "  --clients list      (default 16,32,64)\n"
        << "  --values list       (bytes; default 64,256,1024)\n"
        << "  --duration S        (default 30)\n"
        << "  --out file.csv      (default results.csv)\n";
}

static std::vector<int> parse_list(std::string s) {
    std::vector<int> v;
    std::stringstream ss(std::move(s));
    std::string t;

    while (std::getline(ss, t, ',')) {
        if (!t.empty()) {
            v.push_back(std::stoi(t));
        }
    }

    return v;
}

static void parse_args(int argc, char **argv, Args &a) {
    auto need = [&](int i) {
        if (i >= argc) {
            usage(argv[0]);
            std::exit(1);
        }
    };
    for (int i = 1; i < argc; i++) {
        std::string k = argv[i];

        if (k == "--name1") {
            i++;
            need(i);
            a.t1.name = argv[i];
        } else if (k == "--host1") {
            i++;
            need(i);
            a.t1.host = argv[i];
        } else if (k == "--port1") {
            i++;
            need(i);
            a.t1.port = std::stoi(argv[i]);
        } else if (k == "--name2") {
            i++;
            need(i);
            a.t2.name = argv[i];
        } else if (k == "--host2") {
            i++;
            need(i);
            a.t2.host = argv[i];
        } else if (k == "--port2") {
            i++;
            need(i);
            a.t2.port = std::stoi(argv[i]);
        } else if (k == "--keys") {
            i++;
            need(i);
            a.keys = std::stoull(argv[i]);
        } else if (k == "--warmup-sec") {
            i++;
            need(i);
            a.warmup_sec = std::stoi(argv[i]);
        } else if (k == "--clients") {
            i++;
            need(i);
            a.clients = parse_list(argv[i]);
        } else if (k == "--values") {
            i++;
            need(i);
            a.values = parse_list(argv[i]);
        } else if (k == "--duration") {
            i++;
            need(i);
            a.duration = std::stoi(argv[i]);
        } else if (k == "--out") {
            i++;
            need(i);
            a.out_csv = argv[i];
        } else {
            usage(argv[0]);
            std::exit(1);
        }
    }
}

int main(int argc, char **argv) {
    std::signal(SIGINT, on_sigint);

    Args a;
    parse_args(argc, argv, a);

    std::cout << "Target#1 " << a.t1.name << " " << a.t1.host << ":" << a.t1.port << "\n";
    std::cout << "Target#2 " << a.t2.name << " " << a.t2.host << ":" << a.t2.port << "\n";

    try {
        BenchEngine engine;
        engine.prepare(a);
        std::vector<BenchRow> results = engine.run_matrix(a);

        CsvWriter csv(a.out_csv);
        csv.write_header();

        csv.write(results);
    } catch (const std::exception &e) {
        std::cerr << "ERROR: " << e.what() << "\n";
        return 1;
    }

    std::cout << "Done -> " << a.out_csv << "\n";
    return 0;
}