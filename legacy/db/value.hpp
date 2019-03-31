#pragma once
#include <kj/debug.h>
#include <kj/filesystem.h>
#include <functional>
#include <iostream>
#include <utility>
#include <vector>
#include "db/json.hpp"
#include "db/util.hpp"

namespace db {

namespace detail {

template <typename U, typename T, typename = void>
class Value;

// TODO: specialize this for (unordered_)maps/sets
template <typename U, typename T, typename = void>
class ValueEditor {
 public:
  ValueEditor(const ValueEditor&) = delete;
  ValueEditor& operator=(const ValueEditor&) = delete;
  ValueEditor(ValueEditor&& other) { *this = std::move(other); }
  ValueEditor& operator=(ValueEditor&& other) {
    if (this == &other) return *this;
    obj = other.obj;
    val = std::move(other.val);
    old = std::move(other.old);
    autocommit = other.autocommit;
    finalized = other.finalized;
    rolled_back = other.rolled_back;
    other.finalized = true;
    other.rolled_back = true;
    other.obj = nullptr;
    return *this;
  }
  const T& operator*() const {
    KJ_REQUIRE(!finalized);
    return val;
  }
  const T* operator->() const {
    KJ_REQUIRE(!finalized);
    return &val;
  }

  T& operator*() {
    KJ_REQUIRE(!finalized);
    return val;
  }
  T* operator->() {
    KJ_REQUIRE(!finalized);
    return &val;
  }

  bool Commit() {
    KJ_REQUIRE(!finalized);
    if (obj) obj->is_edited = false;
    bool ret = true;
    if (obj) {
      ret = obj->Commit(val, old);
    }
    finalized = true;
    if (!ret) {
      rolled_back = true;
    }
    return ret;
  }

  void Rollback() {
    KJ_REQUIRE(!rolled_back);
    rolled_back = true;
    if (finalized) {
      UndoCommit();
    }
    finalized = true;
  }

  void UndoCommit() {
    KJ_REQUIRE(finalized);
    if (obj) obj->UndoCommit(old);
  }

  ~ValueEditor() {
    if (!finalized && autocommit) Commit();
    if (obj) obj->is_edited = false;
  }

  ValueEditor(Value<U, T>* obj, T val, bool autocommit)
      : obj(obj), val(val), autocommit(autocommit) {}

 protected:
  Value<U, T>* obj;
  T val;
  T old;
  bool autocommit;
  bool finalized = false;
  bool rolled_back = false;
};

template <typename U, typename T>
class ValueEditor<U, T, std::enable_if_t<T::kIsAlsoValue>> : public T::Editor {
 public:
  using T::Editor::Editor;

  using T::Editor::Commit;
  using T::Editor::Rollback;
  using T::Editor::UndoCommit;
};

template <typename U, typename T, typename>
class Value {
 public:
  const constexpr static bool SkipSerialize = false;
  // Returns true if successful.
  using callback_t = std::function<bool(const T&, const T&)>;
  // Should never fail, as it would leave everything in an inconsistent state.
  using revert_callback_t = std::function<void(const T&, const T&)>;
  Value(kj::Maybe<kj::Own<const kj::Directory>>&& dir, const char* field_name,
        U* parent, T&& v)
      : v(std::move(v)) {}
  Value(kj::Maybe<kj::Own<const kj::Directory>>&& dir, const char* field_name,
        U* parent, const T& v)
      : v(v) {}
  Value(util::JsonConstructorTag, kj::Maybe<kj::Own<const kj::Directory>>&& dir,
        const char* field_name, U* parent, const json& j)
      : Value(std::move(dir), field_name, parent, db::FromJson<T>()(j)) {}
  json Serialize() const { return ToJson<T>()(v); }

  friend class ValueEditor<U, T>;

  bool operator==(const T& v) const { return this->v == v; }
  const T& operator*() const { return v; }
  const T* operator->() const { return &v; }
  operator T() const { return v; }
  operator const T&() const { return v; }
  void OnChange(callback_t action, revert_callback_t revert =
                                       [](const auto&, const auto&) {}) const {
    on_commit.push_back(action);
    on_undo_commit.push_back(revert);
  }

  void SetDir(kj::Maybe<kj::Own<const kj::Directory>>&& dir,
              const char* field_name) {}

  ValueEditor<U, T> Edit(bool autocommit = false) {
    KJ_REQUIRE(!is_edited);
    is_edited = true;
    return ValueEditor<U, T>(this, v, autocommit);
  }

  static auto FromJson(kj::Maybe<kj::Own<const kj::Directory>>&& dir,
                       const char* field_name, U* parent, const json& j) {
    return std::make_unique<Value>(util::JsonConstructorTag(), std::move(dir),
                                   field_name, parent, j);
  }

  static auto Load(kj::Own<const kj::Directory>&& dir, const char* field_name,
                   U* parent) {
    return FromJson(dir->clone(), field_name, parent,
                    json::parse(util::SubDir(std::move(dir), field_name)
                                    ->openFile(kj::Path("data.json"))
                                    ->readAllText()
                                    .cStr()));
  }

  const constexpr static bool kIsSubObject = false;

  template <typename GetObject, typename Fun>
  static void Visit(std::vector<std::string>&, const GetObject&, const Fun&) {}

 private:
  T v;
  bool is_edited = false;

  // Doesn't do anything if the value did not change.
  bool Commit(T val, T& old) {
    is_edited = false;
    old = this->v;
    if constexpr (util::is_equality_comparable_v<T>) {
      if (val == v) return true;
    }
    this->v = val;
    try {
      bool ret = util::propagate_callback_safe(on_commit, on_undo_commit, old,
                                               this->v);
      if (!ret) {
        this->v = old;
      }
      return ret;
    } catch (std::exception& exc) {
      this->v = old;
      throw;
    }
  }

  // Doesn't do anything if the value did not change.
  void UndoCommit(T old) noexcept {
    if constexpr (util::is_equality_comparable_v<T>) {
      if (old == v) return;
    }
    T rollbacked = this->v;
    this->v = old;
    for (const auto& f : on_undo_commit) f(old, rollbacked);
  }
  mutable std::vector<callback_t> on_commit;
  mutable std::vector<revert_callback_t> on_undo_commit;
  bool initialized_ = false;
};

template <typename U, typename T>
class Value<U, T, std::enable_if_t<T::kIsAlsoValue>> : public T {
 public:
  using T::T;
  const T& operator*() const { return *this; }
  using T::Edit;
  static auto FromJson(kj::Maybe<kj::Own<const kj::Directory>>&& dir,
                       const char* field_name, U* parent, const json& j) {
    return std::make_unique<Value>(util::JsonConstructorTag(), std::move(dir),
                                   field_name, parent, j);
  }

  static auto Load(kj::Own<const kj::Directory>&& dir, const char* field_name,
                   U* parent) {
    return FromJson(dir->clone(), field_name, parent,
                    json::parse(util::SubDir(std::move(dir), field_name)
                                    ->openFile(kj::Path("data.json"))
                                    ->readAllText()
                                    .cStr()));
  }
};

}  // namespace detail

template <typename T, typename U>
using Value = detail::Value<T, U>;

};  // namespace db
