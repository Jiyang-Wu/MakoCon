#include "kv_store.h"

KVStore::KVStore() {
}

KVStore::~KVStore() {
}

KVStore::Result KVStore::get(const std::string& key) const {
    auto it = store_.find(key);
    if (it != store_.end()) {
        return Result(it->second, true);
    } else {
        return Result(false);
    }
}

KVStore::Result KVStore::set(const std::string& key, const std::string& value) {
    store_[key] = value;
    return Result("OK", true);
}

KVStore::Result KVStore::execute_operation(const std::string& operation, const std::string& key, const std::string& value) {
    if (operation == "get") {
        return get(key);
    } else if (operation == "set") {
        return set(key, value);
    } else {
        return Result("ERROR: Invalid operation", false);
    }
}

size_t KVStore::size() const {
    return store_.size();
}

bool KVStore::empty() const {
    return store_.empty();
}

void KVStore::clear() {
    store_.clear();
}