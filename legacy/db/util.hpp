#pragma once
#include <kj/debug.h>
#include <kj/filesystem.h>
#include <functional>
#include <tuple>
#include <vector>

namespace db {
namespace util {
class JsonConstructorTag {};

namespace detail {

template <typename T, typename = void>
struct is_equality_comparable : std::false_type {};

template <typename T>
struct is_equality_comparable<
    T,
    typename std::enable_if_t<std::is_same_v<
        decltype(std::declval<const T&>() == std::declval<const T&>()), bool>>>
    : std::true_type {};

}  // namespace detail

template <typename T>
const constexpr bool is_equality_comparable_v =
    detail::is_equality_comparable<T>::value;

template <typename... Args>
bool propagate_callback_safe(
    const std::vector<std::function<bool(Args...)>>& callback,
    const std::vector<std::function<void(Args...)>>& undo_callback,
    std::remove_reference_t<Args>&... args) {
  KJ_ASSERT(callback.size() == undo_callback.size());
  size_t called = 0;
  try {
    for (; called < callback.size(); called++) {
      if (!callback[called](args...)) {
        try {
          for (size_t i = 0; i < called; i++) {
            undo_callback[i](args...);
          }
        } catch (std::exception& exc) {
          std::terminate();
        }
        return false;
      }
    }
    return true;
  } catch (...) {
    for (size_t i = 0; i < called; i++) {
      undo_callback[i](args...);
    }
    throw;
  }
}

template <typename T>
struct argument_type;

template <typename T, typename U>
struct argument_type<T(U)> {
  typedef U type;
};

inline kj::Maybe<kj::Own<const kj::Directory>> CloneDir(
    kj::Maybe<kj::Own<const kj::Directory>>& dir) {
  KJ_IF_MAYBE(d, dir) { return (*d)->clone(); }
  else {
    return nullptr;
  }
}

inline kj::Maybe<kj::Own<const kj::Directory>> SubDir(
    kj::Maybe<kj::Own<const kj::Directory>>& dir, const char* name) {
  if (!name || !name[0]) return CloneDir(dir);
  KJ_IF_MAYBE(d, dir) {
    return (*d)->openSubdir(kj::Path{name}, kj::WriteMode::CREATE |
                                                kj::WriteMode::CREATE_PARENT |
                                                kj::WriteMode::MODIFY);
  }
  else {
    return nullptr;
  }
}

inline kj::Own<const kj::Directory> SubDir(kj::Own<const kj::Directory>&& dir,
                                           const char* name) {
  if (!name || !name[0]) return dir->clone();
  return dir->openSubdir(kj::Path{name}, kj::WriteMode::CREATE |
                                             kj::WriteMode::CREATE_PARENT |
                                             kj::WriteMode::MODIFY);
}
}  // namespace util
}  // namespace db
