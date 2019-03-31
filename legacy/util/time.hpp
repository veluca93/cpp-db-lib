#pragma once
#include <chrono>

namespace util {

using tm_time_t = std::chrono::system_clock::time_point;

inline tm_time_t Epoch() { return std::chrono::system_clock::from_time_t(0); }

inline double ToTimestamp(tm_time_t t) {
  return std::chrono::duration<double>(t - Epoch()).count();
}

inline tm_time_t FromTimestamp(double t) {
  return Epoch() +
         std::chrono::duration_cast<std::chrono::system_clock::duration>(
             std::chrono::duration<double>(t));
}

inline tm_time_t Now() { return std::chrono::system_clock::now(); }

}  // namespace util
