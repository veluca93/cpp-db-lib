#pragma once
#include <kj/filesystem.h>
#include <tuple>
#include <utility>
#include "db/json.hpp"
#include "db/util.hpp"
#include "db/value.hpp"

// NOTE: do not declare containers of Values as members. Instead, use
// Collection.
#define DECLARE_MEMBER_N(tp, name_, json_name)                                 \
  template <typename T>                                                        \
  struct name_##_m {                                                           \
    using type_ = typename util::argument_type<void(tp)>::type;                \
    using value_type_ = db::Value<T, type_>;                                   \
                                                                               \
   protected:                                                                  \
    using parent_t = T;                                                        \
    static constexpr const char* json_name_ = json_name;                       \
                                                                               \
    template <typename... Args>                                                \
    name_##_m(Args... args) : name_##_priv(std::move(args)...) {}              \
    name_##_m(util::JsonConstructorTag(),                                      \
              kj::Maybe<kj::Own<const kj::Directory>>&& dir, T* parent,        \
              const json& j)                                                   \
        : name_##_priv(util::JsonConstructorTag(), std::move(dir), json_name_, \
                       parent, j) {}                                           \
                                                                               \
    json Serialize() const { return name_.Serialize(); }                       \
                                                                               \
    struct Editor_ : protected db::detail::ValueEditor<T, type_> {             \
     protected:                                                                \
      using BaseCls = db::detail::ValueEditor<T, type_>;                       \
      using type = typename name_##_m::type_;                                  \
      friend name_##_m<T>;                                                     \
      template <typename U>                                                    \
      using parent_t = name_##_m<U>;                                           \
      Editor_(db::detail::ValueEditor<T, type_> e) : BaseCls(std::move(e)) {}  \
      Editor_(Editor_&& e) : BaseCls(std::move(e)) {}                          \
                                                                               \
     public:                                                                   \
      db::detail::ValueEditor<T, type_>& name_ = *this;                        \
    };                                                                         \
                                                                               \
   protected:                                                                  \
    auto* Ptr() { return &name_##_priv; }                                      \
    auto& Raw() { return name_##_priv; }                                       \
    const auto& Raw() const { return name_##_priv; }                           \
    const auto& Get() const { return *Raw(); }                                 \
    auto Edit() { return Editor_(name_##_priv.Edit(/*autocommit=*/false)); }   \
                                                                               \
   private:                                                                    \
    value_type_ name_##_priv;                                                  \
                                                                               \
   public:                                                                     \
    using name_##_t = db::detail::Value<T, type_>;                             \
    typename ::db::detail::member_ref<value_type_>::type name_ = name_##_priv; \
  };

#define DECLARE_MEMBER(type, name) DECLARE_MEMBER_N(type, name, #name)

namespace db {

template <typename U, template <typename T> class... Args>
class Data;

namespace detail {
template <typename T, typename = void>
struct member_ref {
  using type = const T&;
};

template <typename U, typename T>
struct member_ref<db::Value<U, T>, std::enable_if_t<T::kIsSubObject>> {
  using type = T&;
};

template <typename U, typename... Args>
class DataEditor : public Args... {
  template <typename T>
  void TryUndoCommit(size_t& done) {
    if (!done) return;
    this->T::Rollback();
    done--;
  }
  template <typename T>
  void TryCommit(size_t& done, bool& fail) {
    if (fail) return;
    if (this->T::Commit()) {
      done++;
    } else {
      fail = true;
    }
  }

  void UndoAllCommits(size_t& done) { (TryUndoCommit<Args>(done), ...); }

 protected:
  DataEditor(DataEditor&& other) : Args(std::move((Args&)other))... {
    obj = other.obj;
    autocommit_ = other.autocommit_;
    finalized_ = other.finalized_;
    rolled_back_ = other.rolled_back_;
    other.finalized_ = true;
    other.rolled_back_ = true;
    other.obj = nullptr;
  }
  DataEditor& operator=(DataEditor&& other) {
    ((this->Args::editor_ = std::move((Args&)other)), ...);
    if (this == &other) return *this;
    obj = other.obj;
    autocommit_ = other.autocommit_;
    finalized_ = other.finalized_;
    rolled_back_ = other.rolled_back_;
    other.finalized_ = true;
    other.rolled_back_ = true;
    other.obj = nullptr;
    return *this;
  }

  bool Commit() {
    KJ_REQUIRE(!finalized_);
    finalized_ = true;
    size_t done = 0;
    bool fail = false;
    try {
      (TryCommit<Args>(done, fail), ...);
    } catch (...) {
      UndoAllCommits(done);
      throw;
    }
    if (fail) {
      try {
        UndoAllCommits(done);
      } catch (std::exception& exc) {
        std::terminate();
      }
    } else {
      KJ_ASSERT(done == sizeof...(Args));
      if (obj) {
        try {
          if (!obj->Commit()) {
            try {
              UndoAllCommits(done);
            } catch (std::exception& exc) {
              std::terminate();
            }
            return false;
          } else {
            return true;
          }
        } catch (std::exception& exc) {
          UndoAllCommits(done);
          throw;
        }
      }
    }
    return !fail;
  }

  void Rollback() {
    KJ_REQUIRE(!rolled_back_);
    rolled_back_ = true;
    if (finalized_) {
      UndoCommit();
    }
    finalized_ = true;
  }

  void UndoCommit() {
    KJ_REQUIRE(finalized_);
    size_t done = sizeof...(Args);
    UndoAllCommits(done);
    if (obj) {
      obj->UndoCommit();
    }
  }

  ~DataEditor() {
    if (!finalized_ && autocommit_) Commit();
  }

  DataEditor(Data<U, Args::template parent_t...>* obj, bool autocommit,
             Args... args)
      : Args(std::move(args))..., obj(obj), autocommit_(autocommit) {}

  friend Data<U, Args::template parent_t...>;

 protected:
  Data<U, Args::template parent_t...>* obj;
  bool autocommit_;
  bool finalized_ = false;
  bool rolled_back_ = false;
};

}  // namespace detail

template <typename U, template <typename T> class... Args>
class Data : public Args<Data<U, Args...>>... {
 public:
  // No move constructor. Use unique pointers.
  Data(Data&&) = delete;

  template <typename... T>
  class BuilderClass {
   public:
    BuilderClass(T... args) : args(std::move(args)...) {}
    BuilderClass&& SetParent(U* parent_) {
      parent = parent_;
      return std::move(*this);
    }
    BuilderClass&& SetDir(kj::Maybe<kj::Own<const kj::Directory>> dir_) {
      dir = std::move(dir_);
      return std::move(*this);
    }
    BuilderClass&& SetField(const char* name_) {
      field_name = name_;
      return std::move(*this);
    }
    template <size_t N>
    auto& Get() {
      return std::get<N>(args);
    }

    std::tuple<T...> args;
    U* parent = nullptr;
    kj::Maybe<kj::Own<const kj::Directory>> dir = nullptr;
    const char* field_name = nullptr;
  };

  // Workaround for https://gcc.gnu.org/bugzilla/show_bug.cgi?id=79501
  template <typename... T>
  static BuilderClass<T...> Builder(T... t) {
    return BuilderClass<T...>(std::move(t)...);
  }

  Data(util::JsonConstructorTag, kj::Maybe<kj::Own<const kj::Directory>> dir,
       const char* field_name, U* parent, const json& js)
      : Args<Data<U, Args...>>(util::JsonConstructorTag(),
                               util::SubDir(dir, field_name),
                               Args<Data<U, Args...>>::json_name_, this,
                               js.at(Args<Data<U, Args...>>::json_name_))...,
        dir_(util::SubDir(dir, field_name)),
        parent_(parent) {}

  template <typename... T>
  Data(kj::Maybe<kj::Own<const kj::Directory>> dir, const char* field_name,
       U* parent, BuilderClass<T...> builder)
      : Data(std::move(builder.SetDir(std::move(dir))
                           .SetField(field_name)
                           .SetParent(parent))) {}

  template <typename... T, std::size_t... Is>
  Data(std::index_sequence<Is...>, BuilderClass<T...> builder)
      : Args<Data<U, Args...>>(util::SubDir(builder.dir, builder.field_name),
                               Args<Data<U, Args...>>::json_name_, this,
                               std::move(builder.template Get<Is>()))...,
        dir_(util::SubDir(builder.dir, builder.field_name)),
        parent_(builder.parent) {
    Commit();
  }

  template <typename... T>
  explicit Data(BuilderClass<T...> builder)
      : Data(std::index_sequence_for<T...>{}, std::move(builder)) {}

  json Serialize() const {
    json j;
    ((void)(Args<Data<U, Args...>>::value_type_::SkipSerialize ||
            (j[std::string(Args<Data<U, Args...>>::json_name_)] =
                 this->Args<Data<U, Args...>>::Serialize(),
             true)),
     ...);
    return j;
  }

  void SetDir(kj::Maybe<kj::Own<const kj::Directory>>&& dir,
              const char* field_name) {
    KJ_IF_MAYBE(d, dir_) {
      KJ_FAIL_ASSERT("SetDir should only be called when dir is null");
    }
    dir_ = util::SubDir(dir, field_name);
    Commit();
  }

#define DECLARE_OPERATOR(op)                                        \
  bool operator op(const Data& other) const {                       \
    return std::tuple<decltype(Args<Data<U, Args...>>::Get())&...>( \
        Args<Data<U, Args...>>::Get()...) op                        \
    std::tuple<decltype(Args<Data<U, Args...>>::Get())&...>(        \
        other.Args<Data<U, Args...>>::Get()...);                    \
  }
  DECLARE_OPERATOR(==);
  DECLARE_OPERATOR(<);
  DECLARE_OPERATOR(>);
  DECLARE_OPERATOR(<=);
  DECLARE_OPERATOR(>=);
  DECLARE_OPERATOR(!=);

  // Returns true if successful.
  using callback_t = std::function<bool()>;
  // Should never fail, as it would leave everything in an inconsistent state.
  using revert_callback_t = std::function<void()>;
  void OnChange(callback_t action, revert_callback_t revert = []() {}) {
    on_commit.push_back(action);
    on_undo_commit.push_back(revert);
  }

  friend detail::DataEditor<U, typename Args<Data<U, Args...>>::Editor_...>;

  template <template <typename> class T>
  typename T<Data<U, Args...>>::value_type_& Get() {
    return this->T<Data<U, Args...>>::Raw();
  }

  template <template <typename> class T>
  const typename T<Data<U, Args...>>::value_type_& Get() const {
    return this->T<Data<U, Args...>>::Raw();
  }

  U* Parent() { return parent_; }
  const U* Parent() const { return parent_; }
  using ParentType = U;

  const constexpr static bool kIsAlsoValue = true;
  const constexpr static bool kIsSubObject = true;
  const constexpr static bool SkipSerialize = false;
  using Editor = detail::DataEditor<U, typename Args<Data>::Editor_...>;

  auto Edit(bool autocommit = false) {
    return detail::ValueEditor<U, Data>(this, autocommit,
                                        this->Args<Data>::Edit()...);
  }

  const kj::Maybe<kj::Own<const kj::Directory>>& Directory() const {
    return dir_;
  }

  template <typename GetObject, typename Fun>
  static void Visit(std::vector<std::string>& path, const GetObject& get_object,
                    const Fun& reg) {
    reg(path, get_object);
    (VisitSingle<Args<Data>>(path, get_object, reg), ...);
  }

 private:
  template <typename A, typename GetObject, typename Fun>
  static void VisitSingle(std::vector<std::string>& path,
                          const GetObject& get_object, const Fun& reg) {
    path.push_back(A::json_name_);
    KJ_DEFER(path.pop_back());
    A::value_type_::Visit(path,
                          [get_object](const std::vector<std::string>& path) ->
                          typename A::value_type_* {
                            auto* obj = get_object(path);
                            if (!obj) return nullptr;
                            return obj->A::Ptr();
                          },
                          reg);
  }
  bool Commit() {
    if (!util::propagate_callback_safe(on_commit, on_undo_commit)) return false;
    KJ_IF_MAYBE(d, dir_) {
      auto replacer = (*d)->replaceFile(
          kj::Path("data.json"), kj::WriteMode::CREATE | kj::WriteMode::MODIFY);
      replacer->get().writeAll(std::string(Serialize().dump()).c_str());
      replacer->commit();
    }
    return true;
  }

  void UndoCommit() noexcept {
    for (const auto& f : on_undo_commit) f();
    KJ_IF_MAYBE(d, dir_) {
      auto replacer = (*d)->replaceFile(
          kj::Path("data.json"), kj::WriteMode::CREATE | kj::WriteMode::MODIFY);
      replacer->get().writeAll(std::string(Serialize().dump()).c_str());
      replacer->commit();
    }
  }
  kj::Maybe<kj::Own<const kj::Directory>> dir_;
  std::vector<callback_t> on_commit;
  std::vector<revert_callback_t> on_undo_commit;
  U* parent_;
  friend U;
};

template <template <typename T> class... Args>
using MainData = Value<void, Data<void, Args...>>;

}  // namespace db
