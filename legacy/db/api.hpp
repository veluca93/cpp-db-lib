#pragma once
#include <kj/compat/http.h>
#include <functional>
#include "db/container.hpp"
#include "db/json.hpp"
#include "db/serializable.hpp"

namespace db {

namespace api {

// Summary used by Containers.
template <typename T>
class Summary {
 public:
  static json Get(const T* obj) { return obj->Serialize(); }
};

// Default call policies.
template <typename T, typename Context>
struct CanCallConst {
  static bool Get(Context* context, const T* obj, const json& j) {
    return true;
  }
};

template <typename T, typename Context>
struct CanCall {
  static bool Get(Context* context, const T* obj, const json& j) {
    return false;
  }
};

// Utility functions for implementation
inline kj::Promise<void> Error(kj::HttpService::Response& resp, int status,
                               const char* msg,
                               const char* json_msg = nullptr) {
  if (json_msg == nullptr) json_msg = msg;
  static kj::HttpHeaderTable empty_table_;
  json res;
  res["code"] = status;
  res["error"] = json_msg;
  KJ_LOG(WARNING, json_msg);
  auto data = kj::str(res.dump().c_str());
  kj::HttpHeaders answer_headers(empty_table_);
  answer_headers.add("Content-Type", "application/json");
  auto ans = resp.send(status, msg, answer_headers, data.size());
  return ans->write(data.cStr(), data.size())
      .attach(std::move(ans), std::move(data));
}

inline kj::Promise<void> AnswerJson(kj::HttpService::Response& resp,
                                    const json& j) {
  static kj::HttpHeaderTable empty_table_;
  json res;
  res["result"] = j;
  auto data = kj::str(res.dump().c_str());
  kj::HttpHeaders answer_headers(empty_table_);
  answer_headers.add("Content-Type", "application/json");
  auto ans = resp.send(200, "OK", answer_headers, data.size());
  return ans->write(data.cStr(), data.size())
      .attach(std::move(ans), std::move(data));
}

inline kj::Promise<void> AnswerRaw(kj::HttpService::Response& resp,
                                   const char* content, size_t len,
                                   const char* content_type) {
  static kj::HttpHeaderTable empty_table_;
  auto data = kj::heapString(content, len);
  kj::HttpHeaders answer_headers(empty_table_);
  answer_headers.add("Content-Type", content_type);
  auto ans = resp.send(200, "OK", answer_headers, data.size());
  return ans->write(data.cStr(), data.size())
      .attach(std::move(ans), std::move(data));
}

template <typename T, typename Context>
class BaseAPIHandler {
 public:
  using ConstAPICall = std::function<kj::Promise<void>(
      Context*, const T*, const json&, kj::HttpService::Response&)>;
  using APICall = std::function<kj::Promise<void>(Context*, T*, const json&,
                                                  kj::HttpService::Response&)>;

  static void RegisterConstAPI(std::string name, const ConstAPICall& call) {
    ConstRegistry()[name] = call;
  }

  static void RegisterAPI(std::string name, const APICall& call) {
    Registry()[name] = call;
  }

  static std::pair<std::vector<std::string>, std::vector<std::string>>
  ListAPI() {
    std::vector<std::string> capi;
    std::vector<std::string> ncapi;
    for (const auto& [k, v] : ConstRegistry()) {
      capi.push_back(k);
    }
    for (const auto& [k, v] : Registry()) {
      ncapi.push_back(k);
    }
    return {capi, ncapi};
  }

  static kj::Promise<void> Dispatch(Context* context, T* obj, const json& j,
                                    kj::HttpService::Response& resp) {
    if (!obj) return Error(resp, 404, "Not Found");
    if (!j.is_object()) return Error(resp, 400, "Bad Request");
    if (j.find("action") == j.end()) return Error(resp, 400, "Bad Request");
    std::string action = j.at("action");
    if (ConstRegistry().count(action)) {
      if (!CanCallConst<T, Context>::Get(context, obj, j))
        return Error(resp, 403, "Forbidden");
      return ConstRegistry().at(action)(context, obj, j, resp);
    }
    if (Registry().count(action)) {
      if (!CanCall<T, Context>::Get(context, obj, j))
        return Error(resp, 403, "Forbidden");
      return Registry().at(action)(context, obj, j, resp);
    }
    return Error(resp, 404, "Not Found");
  }

 private:
  static std::unordered_map<std::string, ConstAPICall>& ConstRegistry() {
    static std::unordered_map<std::string, ConstAPICall> r;
    return r;
  }
  static std::unordered_map<std::string, APICall>& Registry() {
    static std::unordered_map<std::string, APICall> r;
    return r;
  }
};

template <typename T, typename Context>
class API : public BaseAPIHandler<T, Context> {
 public:
  static void Register() {
    throw std::runtime_error("Please specialize this template class");
  }
};

template <typename Context, typename U, template <typename T> class... Args>
class API<Data<U, Args...>, Context>
    : public BaseAPIHandler<Data<U, Args...>, Context> {
  using T = Data<U, Args...>;
  using B = BaseAPIHandler<T, Context>;

 public:
  static kj::Promise<void> Get(Context* context, const T* obj, const json& j,
                               kj::HttpService::Response& resp) {
    return AnswerJson(resp, obj->Serialize());
  }
  static void Register() { B::RegisterConstAPI("get", &Get); }
};

template <typename Context,
          template <typename, template <typename> class,
                    template <typename> class, typename...>
          class Config,
          typename U, template <typename> class T_,
          template <typename> class Key, typename... Other>
class API<::db::detail::Value<
              U, ::db::detail::BaseContainer<Config, U, T_, Key, Other...>>,
          Context>
    : public BaseAPIHandler<
          ::db::detail::Value<
              U, ::db::detail::BaseContainer<Config, U, T_, Key, Other...>>,
          Context> {
  using T = ::db::detail::Value<
      U, ::db::detail::BaseContainer<Config, U, T_, Key, Other...>>;
  using B = BaseAPIHandler<T, Context>;

 public:
  static kj::Promise<void> List(Context* context, const T* obj, const json& j,
                                kj::HttpService::Response& resp) {
    json r = json::array();
    for (const auto& [k, v] : *obj) {
      r[k] = Summary<typename T::Contained>::Get(&*v);
    }
    return AnswerJson(resp, r);
  }
  static void Register() { B::RegisterConstAPI("list", &List); }
};

}  // namespace api

}  // namespace db
