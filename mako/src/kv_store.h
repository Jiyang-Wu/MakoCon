#ifndef _KV_STORE_H_
#define _KV_STORE_H_

#include <string>
#include <map>
#include <utility>

class KVStore {
public:
    KVStore();
    ~KVStore();
    
    struct Result {
        std::string value;
        bool success;
        
        Result(const std::string& val, bool succ) : value(val), success(succ) {}
        Result(bool succ) : value(""), success(succ) {}
    };
    
    Result get(const std::string& key) const;
    Result set(const std::string& key, const std::string& value);
    Result execute_operation(const std::string& operation, const std::string& key, const std::string& value);
    
    size_t size() const;
    bool empty() const;
    void clear();
    
private:
    std::map<std::string, std::string> store_;
};

#endif