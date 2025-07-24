// kv_store.h
#ifndef KV_STORE_H
#define KV_STORE_H

#include <string>
#include <shared_mutex>
#include <map>
#include <mutex>

class KVStore {
public:
    KVStore() = default;
    ~KVStore() = default;

    void set(const std::string& key, const std::string& value);
    bool get(const std::string& key, std::string& value) const;
    bool del(const std::string& key);

private:
    mutable std::shared_mutex mutex_;
    std::map<std::string, std::string> store_;
};

#endif // KV_STORE_H