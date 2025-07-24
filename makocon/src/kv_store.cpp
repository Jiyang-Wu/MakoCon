// kv_store.cpp
#include "kv_store.h"

void KVStore::set(const std::string& key, const std::string& value) {
    std::unique_lock lock(mutex_);
    store_[key] = value;
}

bool KVStore::get(const std::string& key, std::string& value) const {
    std::shared_lock lock(mutex_);
    auto it = store_.find(key);
    if (it != store_.end()) {
        value = it->second;
        return true;
    }
    return false;
}

bool KVStore::del(const std::string& key) {
    std::unique_lock lock(mutex_);
    return store_.erase(key) > 0;
}
