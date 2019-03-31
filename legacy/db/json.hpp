#pragma once
#include <nlohmann/json.hpp>
#include <unordered_set>
#include "util/time.hpp"

namespace db {
using json = nlohmann::json;

template <typename T, typename = void>
struct FromJson;

template <typename T>
struct FromJson<T, std::enable_if_t<std::is_arithmetic_v<T>>> {
  T operator()(const json& j) const { return j; }
};

template <>
struct FromJson<std::string> {
  std::string operator()(const json& j) const { return j; }
};

// TODO: generalize to different containers?
template <typename T>
struct FromJson<std::vector<T>> {
  std::vector<T> operator()(const json& j) {
    std::vector<T> res;
    for (const auto& v : j) {
      res.push_back(FromJson<T>()(v));
    }
    return res;
  }
};

template <typename T>
struct FromJson<std::unordered_set<T>> {
  std::unordered_set<T> operator()(const json& j) {
    std::unordered_set<T> res;
    for (const auto& v : j) {
      res.emplace(FromJson<T>()(v));
    }
    return res;
  }
};

template <typename T, typename U>
struct FromJson<std::unordered_map<T, U>> {
  std::unordered_map<T, U> operator()(const json& j) {
    std::unordered_map<T, U> res;
    for (auto it = j.begin(); it != j.end(); ++it) {
      res.emplace(it.key(), FromJson<U>()(it.value()));
    }
    return res;
  }
};

template <>
struct FromJson<json> {
  json operator()(const json& j) { return j; }
};

template <>
struct FromJson<::util::tm_time_t> {
  util::tm_time_t operator()(const json& j) {
    return util::FromTimestamp(j.get<double>());
  }
};

template <typename T, typename = void>
struct ToJson;

template <typename T>
struct ToJson<T, std::enable_if_t<std::is_arithmetic_v<T>>> {
  json operator()(const T& j) const { return j; }
};

template <>
struct ToJson<std::string> {
  json operator()(const std::string& j) const { return j; }
};

// TODO: generalize to different containers?
template <typename T>
struct ToJson<std::vector<T>> {
  json operator()(const std::vector<T>& j) {
    json res;
    for (const auto& v : j) {
      res.push_back(ToJson<T>()(v));
    }
    return res;
  }
};

template <typename T>
struct ToJson<std::unordered_set<T>> {
  json operator()(const std::unordered_set<T>& j) {
    json res;
    for (const auto& v : j) {
      res.push_back(ToJson<T>()(v));
    }
    return res;
  }
};

template <typename T, typename U>
struct ToJson<std::unordered_map<T, U>> {
  json operator()(const std::unordered_map<T, U>& j) {
    json res;
    for (const auto& [k, v] : j) {
      res[k] = ToJson<U>()(v);
    }
    return res;
  }
};

template <>
struct ToJson<json> {
  json operator()(const json& j) { return j; }
};

template <>
struct ToJson<::util::tm_time_t> {
  json operator()(const ::util::tm_time_t t) { return util::ToTimestamp(t); }
};

}  // namespace db
