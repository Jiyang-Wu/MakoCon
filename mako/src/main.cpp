#include "kv_store.h"
#include <iostream>
#include <signal.h>

MakoKVStore* g_kv_store = nullptr;

void signal_handler(int signal) {
    if (g_kv_store) {
        std::cout << "\nShutting down MakoKVStore..." << std::endl;
        g_kv_store->stop();
        delete g_kv_store;
        g_kv_store = nullptr;
    }
    exit(0);
}

int main() {
    // Set up signal handler for graceful shutdown
    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);
    
    std::cout << "Starting Mako KV Store..." << std::endl;
    
    // Create and initialize KV store
    g_kv_store = new MakoKVStore();
    
    if (!g_kv_store->init()) {
        std::cerr << "Failed to initialize KV store" << std::endl;
        delete g_kv_store;
        return 1;
    }
    
    std::cout << "KV Store initialized. Starting request polling..." << std::endl;
    
    // Start the polling thread
    g_kv_store->start_polling();
    
    std::cout << "Mako KV Store is running. Press Ctrl+C to stop." << std::endl;
    
    // Keep main thread alive
    while (true) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
    
    return 0;
}