#pragma once

#include <assert.h>
#include <functional>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <chrono>
#include <deque>

using ClientId = std::string;
using ServerId = std::string;
using Cost = uint32_t;

struct TestRequest {
  ServerId server;
};

struct TestResponse {
};

