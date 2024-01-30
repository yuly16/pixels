//
// Created by yuly on 03.05.23.
//

#ifndef DUCKDB_TIMEPROFILER_H
#define DUCKDB_TIMEPROFILER_H

#include <iostream>
#include <memory>
#include <string>
#include "exception/InvalidArgumentException.h"
#include "profiler/AbstractProfiler.h"
#include <unordered_map>
#include <chrono>
#include <map>
#include <mutex>
#include <thread>


class TimeProfiler: public AbstractProfiler {
public:
    static TimeProfiler & Instance();
    void Start(const std::string& label);
    void End(const std::string& label);
    long Get(const std::string &label);
    void Reset() override;
    void Print() override;
    void push(std::string key, long long value);
    void Collect();
    int GetResultSize();
    std::unordered_map<std::string, long long> colSizes;
    std::mutex m;
private:
    TimeProfiler();
    static thread_local std::map<std::string,std::chrono::steady_clock::time_point> profiling;
    static thread_local std::map<std::string, long> localResult;
    std::mutex lock;
    std::unordered_map<std::string, long> globalResult;
};

#endif //DUCKDB_TIMEPROFILER_H
