#include "rust_wrapper.h"

// Global instance pointer for Rust notification
RustWrapper* g_rust_wrapper_instance = nullptr;

// C function implementation for Rust to call
extern "C" void cpp_notify_request_available() {
    if (g_rust_wrapper_instance) {
        g_rust_wrapper_instance->notify_request_available();
    }
}

RustWrapper::RustWrapper() : running_(false), initialized_(false) {
    g_rust_wrapper_instance = this;
}

RustWrapper::~RustWrapper() {
    stop();
}

bool RustWrapper::init() {
    if (initialized_) {
        return false; // Already initialized
    }
    
    // Initialize Rust socket listener
    if (!rust_init()) {
        std::cerr << "Failed to initialize Rust socket listener" << std::endl;
        return false;
    }
    
    running_ = true;
    initialized_ = true;
    
    std::cout << "RustWrapper initialized successfully" << std::endl;
    return true;
}

void RustWrapper::start_polling() {
    if (!initialized_ || processing_thread_.joinable()) {
        return;
    }
    
    processing_thread_ = std::thread(&RustWrapper::poll_requests, this);
    std::cout << "Started event-driven request processing thread" << std::endl;
}

void RustWrapper::stop() {
    if (running_) {
        running_ = false;
        // Notify the processing thread to wake up and exit
        request_cv_.notify_all();
        if (processing_thread_.joinable()) {
            processing_thread_.join();
        }
    }
    g_rust_wrapper_instance = nullptr;
}

void RustWrapper::notify_request_available() {
    request_cv_.notify_one();
}

void RustWrapper::poll_requests() {
    long int counter = 0;
    
    while (running_) {
        // Wait for notification from Rust
        std::unique_lock<std::mutex> lock(request_mutex_);
        request_cv_.wait(lock);
        
        if (!running_) {
            break;
        }
        
        // Process all available requests
        uint32_t id;
        char* operation = nullptr;
        char* key = nullptr;
        char* value = nullptr;
        
        while (rust_retrieve_request_from_queue(&id, &operation, &key, &value)) {
            counter++;
            std::cout << "Processing request " << id << ": " << operation << ":" << key << ":" << value << std::endl;
            
            // Execute the request
            execute_request(id, 
                           operation ? operation : "", 
                           key ? key : "", 
                           value ? value : "");
            
            // Free the C strings allocated by Rust
            if (operation) rust_free_string(operation);
            if (key) rust_free_string(key);
            if (value) rust_free_string(value);
        }
        
        if (counter % 100 == 0) {
            std::cout << "Processed " << counter << " requests so far" << std::endl;
        }
    }
}

void RustWrapper::execute_request(uint32_t id, const string& operation, const string& key, const string& value) {
    KVStore::Result result = kv_store_.execute_operation(operation, key, value);
    
    // Send response back to Rust
    rust_put_response_back_queue(id, result.value.c_str(), result.success);
    
    std::cout << "Executed " << operation << " for key '" << key << "' -> " << result.value << std::endl;
}