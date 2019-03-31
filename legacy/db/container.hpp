#pragma once
#include <unordered_map>
#include <unordered_set>
#include "db/serializable.hpp"
#include "db/util.hpp"
#include "db/value.hpp"

namespace db {

namespace placeholders {
namespace detail {
// Used to default-construct a Container.
class _ {};
}  // namespace detail
static const constexpr detail::_ _;
}  // namespace placeholders

namespace detail {

template <template <typename, template <typename> class,
                    template <typename> class, typename...>
          class,
          typename, template <typename> class, template <typename> class,
          typename...>
class BaseContainer;

template <typename Type>
class ContainerEditor;

template <typename Owner, typename T>
class OwnerPtr {
 public:
  template <typename... Args>
  static bool IsValidPre(Args&&... args) {
    return true;
  }
  template <typename U>
  static bool IsValidPost(Owner* obj, const U& t) {
    return true;
  }
  template <typename... Args>
  static auto New(Owner* obj, Args... args) {
    return std::make_unique<T>(nullptr, nullptr, obj, std::move(args)...);
  }
  using type = std::unique_ptr<T>;
};

}  // namespace detail

namespace placeholders {
template <typename T>
class parent_ {};

template <typename U, template <typename T> class... Args>
class parent_<Data<U, Args...>> {
 public:
  using type_ = U;
};

template <template <typename, template <typename> class,
                    template <typename> class, typename...>
          class Config,
          typename U, template <typename> class T,
          template <typename> class Key, typename... Other>
class parent_<::db::detail::BaseContainer<Config, U, T, Key, Other...>> {
 public:
  using type_ = U;
};

template <typename T>
class sibling_ {};

template <typename U, template <typename T> class... Args>
class sibling_<Data<U, Args...>> {
 public:
  using type_ = typename U::SiblingType;
};
}  // namespace placeholders

template <typename T, template <typename> class memb>
class member {
 public:
  using type = typename memb<T>::value_type_;
  using inner_type = typename memb<T>::type_;
  type& operator()(T& t) const { return t.template Get<memb>(); }
  const type& ConstGet(const T& t) const { return t.template Get<memb>(); }
};

template <typename T>
class member<T, placeholders::parent_> {
 public:
  using type = typename placeholders::parent_<T>::type_;
  type& operator()(T& t) const { return *t.Parent(); }
  const type& ConstGet(const T& t) const { return *t.Parent(); }
};

template <typename T>
class member<T, placeholders::sibling_> {
 public:
  using type = typename placeholders::sibling_<T>::type_;
  type& operator()(T& t) const {
    return t.Parent()->Sibling(typename T::ParentType::Key_t().ConstGet(t));
  }
  const type& ConstGet(const T& t) const {
    return t.Parent()->Sibling(typename T::ParentType::Key_t().ConstGet(t));
  }
};

namespace detail {
template <typename Type>
class ContainerEditor {
  using Key_t = typename Type::Key_t;
  using KeyType = typename Type::KeyType;
  using Inner = typename Type::Inner;
  using Contained = typename Type::Contained;
  using Ptr = typename Type::Ptr;

 public:
  ContainerEditor(Type* obj, bool autocommit)
      : obj(obj), autocommit(autocommit) {}
  ContainerEditor(ContainerEditor&& other) { *this = std::move(other); }
  ContainerEditor& operator=(ContainerEditor&& other) {
    if (this == &other) return *this;
    obj = other.obj;
    autocommit = other.autocommit;
    finalized = other.finalized;
    rolled_back = other.rolled_back;
    other.finalized = true;
    other.rolled_back = true;
    other.obj = nullptr;
    extra_values = std::move(other.extra_values);
    to_erase = std::move(other.to_erase);
    inserted = std::move(other.inserted);
    erased = std::move(other.erased);
    editors = std::move(other.editors);
    return *this;
  }
  const detail::ValueEditor<Type, Contained>& Get(const KeyType& v) const {
    KJ_REQUIRE(!finalized);
    if (!editors.count(v)) {
      KJ_ASSERT(obj->values.count(v));
      editors.emplace(v, obj->values.at(v)->Edit());
    }
    return editors.at(v);
  }
  detail::ValueEditor<Type, Contained>& Get(const KeyType& v) {
    KJ_REQUIRE(!finalized);
    if (!editors.count(v)) {
      KJ_ASSERT(obj->values.count(v));
      editors.emplace(v, obj->values.at(v)->Edit());
    }
    return editors.at(v);
  }
  bool Count(const KeyType& v) const {
    KJ_REQUIRE(!finalized);
    if (extra_values.count(v)) return true;
    if (to_erase.count(v)) return false;
    return obj->Count(v);
  }
  size_t Size() const {
    KJ_REQUIRE(!finalized);
    return obj->size() + extra_values.size() - to_erase.size();
  }

  template <typename A>
  bool Emplace(A arg) {
    KJ_REQUIRE(!finalized);
    if (!Ptr::IsValidPre(obj, arg)) return false;
    auto temp = Ptr::New(obj, std::move(arg));
    const KeyType& k = Key_t().ConstGet(*temp);
    if (!Ptr::IsValidPost(obj, k)) return false;
    if (Count(k)) return false;
    return extra_values.emplace(k, std::move(temp)).second;
  }

  bool Erase(const KeyType& k) {
    KJ_REQUIRE(!finalized);
    if (!Count(k)) return false;
    if (extra_values.count(k)) {
      KJ_ASSERT(extra_values.erase(k));
      return true;
    }
    return to_erase.emplace(k).second;
  }

  bool Commit() {
    KJ_REQUIRE(!finalized);
    if (obj) obj->is_edited = false;
    bool ret = true;
    if (obj) {
      try {
        for (auto& [k, v] : editors) {
          ret = v.Commit();
          if (!ret) break;
          committed_editors.emplace(k, std::move(v));
        }
        if (ret) {
          for (const auto& e : to_erase) {
            auto tmp = obj->Erase(e);
            if (!tmp) {
              ret = false;
              break;
            }
            erased.emplace(e, std::move(tmp));
          }
        }
        if (ret) {
          for (auto& [k, v] : extra_values) {
            ret = obj->Insert(k, std::move(v));
            if (!ret) {
              break;
            }
            inserted.emplace(k);
          }
        }
      } catch (...) {
        finalized = true;
        rolled_back = true;
        UndoCommit();
        throw;
      }
    }
    finalized = true;
    if (!ret) {
      rolled_back = true;
      UndoCommit();
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
    if (obj) {
      try {
        for (auto& [k, v] : committed_editors) {
          v.UndoCommit();
        }
        for (const auto& e : inserted) {
          KJ_ASSERT(!!obj->Erase(e));
        }
        for (auto& [k, v] : erased) {
          KJ_ASSERT(obj->Insert(k, std::move(v)));
        }
      } catch (std::exception& e) {
        std::terminate();
      }
    }
  }

  ~ContainerEditor() {
    if (!finalized && autocommit) Commit();
    if (obj) obj->is_edited = false;
  }

 private:
  Type* obj;
  bool autocommit;
  bool finalized = false;
  bool rolled_back = false;
  std::unordered_map<KeyType, typename Ptr::type> extra_values;
  std::unordered_set<KeyType> to_erase;
  std::unordered_set<KeyType> inserted;
  std::unordered_map<KeyType, typename Ptr::type> erased;
  std::unordered_map<KeyType, detail::ValueEditor<Type, Contained>> editors;
  std::unordered_map<KeyType, detail::ValueEditor<Type, Contained>>
      committed_editors;
};

// T should be a partial instantiation of Data.
template <template <typename, template <typename> class,
                    template <typename> class, typename...>
          class Config,
          typename U, template <typename> class T,
          template <typename> class Key, typename... Other>
class BaseContainer : public Config<U, T, Key, Other...> {
 protected:
  using ContainerSetup = Config<U, T, Key, Other...>;
  using Inner = typename ContainerSetup::Inner;
  using Ptr = typename ContainerSetup::Ptr;

 public:
  using Contained = typename ContainerSetup::Contained;
  using ContainedRef = typename ContainerSetup::ContainedRef;
  using ParentType = typename ContainerSetup::ParentType;
  using Key_t = typename ContainerSetup::Key_t;
  using KeyType = typename Key_t::inner_type;

  template <typename... Args>
  static auto Builder(Args... args) {
    return Contained::Builder(std::move(args)...);
  }

  const constexpr static bool SkipSerialize = false;
  BaseContainer(kj::Maybe<kj::Own<const kj::Directory>>&& dir,
                const char* field_name, ParentType* parent,
                placeholders::detail::_)
      : dir(util::SubDir(dir, field_name)), parent(parent) {}

  json Serialize() const {
    json j;
    // We only serialize the keys, as the values live in a sub-folder.
    for (const auto& v : values) {
      j.push_back(v.first);
    }
    return j;
  }

  // Allow editing inner values without editing the whole container.
  ContainedRef& Get(const KeyType& v) { return *values.at(v); }
  const Contained& Get(const KeyType& v) const { return *values.at(v); }
  bool Count(const KeyType& v) const { return values.count(v); }
  size_t Size() const { return values.size(); }

  auto begin() const { return values.begin(); }
  auto end() const { return values.end(); }

  const constexpr static bool kIsAlsoValue = true;
  const constexpr static bool kIsSubObject = true;
  using Editor = detail::ContainerEditor<typename ContainerSetup::Self>;
  friend Editor;

  BaseContainer(const util::JsonConstructorTag&,
                kj::Maybe<kj::Own<const kj::Directory>> dir,
                const char* field_name, ParentType* parent, const json& j)
      : BaseContainer(std::move(dir), field_name, parent, placeholders::_) {
    KJ_IF_MAYBE(d, this->dir) {
      if constexpr (ContainerSetup::kRequiresDir) {
        for (const auto& v : j) {
          if constexpr (std::is_same_v<std::string, KeyType>) {
            AddFromKey(*d, v.get<std::string>());
          } else {
            AddFromKey(*d, std::to_string(v.get<KeyType>()));
          }
        }
        return;
      }
    }
    for (const auto& v : j) {
      AddFromKeyWithoutDir(v.get<KeyType>());
    }
  }

  auto Edit(bool autocommit = false) {
    KJ_REQUIRE(!this->is_edited);
    this->is_edited = true;
    return detail::ValueEditor<ParentType, BaseContainer>(this, autocommit);
  }

  void OnInsert(const std::function<bool(const Contained&)>& insert,
                const std::function<void(const Contained&)>& undo_insert =
                    [](auto&) {}) const {
    std::unordered_set<KeyType> done;
    bool failed = false;
    for (const auto& [k, v] : values) {
      try {
        if (!insert(v)) {
          try {
            for (const auto& k : done) {
              undo_insert(values.at(k));
            }
          } catch (...) {
            std::terminate();
          }
          failed = true;
          break;
        }
      } catch (...) {
        for (const auto& k : done) {
          undo_insert(values.at(k));
        }
        throw;
      }
      done.insert(k);
    }
    if (failed)
      throw std::runtime_error("Callback failed on already-present data!");
    on_insert.push_back(insert);
    on_undo_insert.push_back(undo_insert);
  }

  void OnErase(const std::function<bool(const Contained&)>& erase,
               const std::function<void(const Contained&)>& undo_erase =
                   [](auto&) {}) const {
    on_erase.push_back(erase);
    on_undo_erase.push_back(undo_erase);
  }

  bool operator==(const BaseContainer& other) const {
    if (Size() != other.Size()) return false;
    for (const auto& [k, v] : other) {
      if (!Count(k)) return false;
      if (*values.at(k) != *v) return false;
    }
    return true;
  }

  ParentType* Parent() { return parent; }
  const ParentType* Parent() const { return parent; }

  template <typename GetObject, typename Fun>
  static void Visit(std::vector<std::string>& path, const GetObject& get_object,
                    const Fun& reg) {
    // TODO(veluca): introduce an appropriate flag.
    reg(path, get_object);
    if constexpr (ContainerSetup::kRequiresDir) {
      size_t depth = path.size();
      path.push_back(":key");
      KJ_DEFER(path.pop_back());
      Contained::Visit(
          path,
          [depth,
           get_object](const std::vector<std::string>& path) -> Contained* {
            auto* cnt = get_object(path);
            if (!cnt) return nullptr;
            KJ_ASSERT(path.size() > depth);
            if constexpr (std::is_same_v<std::string, KeyType>) {
              KeyType key = path[depth];
              if (!cnt->Count(key)) return nullptr;
              return &cnt->Get(key);
            }
            if constexpr (std::is_same_v<int, KeyType> ||
                          std::is_same_v<long, KeyType> ||
                          std::is_same_v<long long, KeyType>) {
              KeyType key = std::stoll(path[depth]);
              if (!cnt->Count(key)) return nullptr;
              return &cnt->Get(key);
            }
            if constexpr (std::is_same_v<unsigned, KeyType> ||
                          std::is_same_v<unsigned long, KeyType> ||
                          std::is_same_v<unsigned long long, KeyType>) {
              KeyType key = std::stoull(path[depth]);
              if (!cnt->Count(key)) return nullptr;
              return &cnt->Get(key);
            }
          },
          reg);
    }
  }

 protected:
  void AddFromKey(const kj::Directory* d, const std::string& s) {
    if constexpr (!ContainerSetup::kRequiresDir) {
      KJ_FAIL_ASSERT("AddFromKey called, but kRequiresDir is false!");
    } else {
      auto temp = Inner::Load(d->clone(), s.c_str(), this);
      const KeyType& k = Key_t().ConstGet(*temp);
      if constexpr (std::is_same_v<KeyType, std::string>) {
        if (k != s) {
          throw std::runtime_error("Invalid object: " + s);
        }
      } else {
        if (std::to_string(k) != s) {
          throw std::runtime_error("Invalid object: " + s);
        }
      }
      this->values.emplace(k, std::move(temp));
    }
  }

  void AddFromKeyWithoutDir(const KeyType& s) {
    if constexpr (ContainerSetup::kRequiresDir) {
      throw std::runtime_error(
          "Deserializing a Container requires a storage directory!\n");
    } else {
      if (!Ptr::IsValidPost(this, s))
        throw std::runtime_error("Invalid deserialized data!");
      auto temp = Ptr::New(this, s);
      const auto& k = typename ContainerSetup::Key_t().ConstGet(*temp);
      if (this->Count(k))
        throw std::runtime_error("Invalid deserialized data!");
      if (!this->values.emplace(k, std::move(temp)).second)
        throw std::runtime_error("Invalid deserialized data!");
    }
  }

  bool Insert(const KeyType& k, typename Ptr::type&& v) {
    KJ_ASSERT(!!v);
    if (Count(k)) return false;
    if constexpr (ContainerSetup::kRequiresDir) {
      if constexpr (std::is_same_v<KeyType, std::string>) {
        v->SetDir(util::CloneDir(dir), k.c_str());
      } else {
        v->SetDir(util::CloneDir(dir), std::to_string(k).c_str());
      }
    }
    KJ_ASSERT(values.emplace(k, std::move(v)).second);
    Key_t()
        .ConstGet(*values.at(k))
        .OnChange(
            [this](const auto& o, const auto& n) { return ChangeKey(o, n); },
            [this](const auto& o, const auto& n) {
              KJ_ASSERT(ChangeKey(n, o));
            });
    if (!util::propagate_callback_safe(on_insert, on_undo_insert,
                                       *values.at(k)))
      return false;
    return true;
  }

  typename Ptr::type Erase(const KeyType& v) {
    if (!Count(v)) return nullptr;
    auto ret = std::move(values.at(v));
    values.erase(v);
    if (!util::propagate_callback_safe(on_erase, on_undo_erase, *ret)) {
      KJ_ASSERT(values.emplace(v, std::move(ret)).second);
      return nullptr;
    }
    return ret;
  }

  bool ChangeKey(const KeyType& o, const KeyType& n) {
    if constexpr (util::is_equality_comparable_v<KeyType>) {
      if (o == n) return true;
    }
    if (Count(n)) return false;
    if (!Count(o)) return false;
    auto node = values.extract(o);
    node.key() = n;
    KJ_ASSERT(values.insert(std::move(node)).inserted);
    return true;
  }

  bool is_edited = false;
  std::unordered_map<KeyType, typename Ptr::type> values;
  kj::Maybe<kj::Own<const kj::Directory>> dir;
  ParentType* parent;
  mutable std::vector<std::function<bool(const Contained&)>> on_erase;
  mutable std::vector<std::function<void(const Contained&)>> on_undo_erase;
  mutable std::vector<std::function<bool(const Contained&)>> on_insert;
  mutable std::vector<std::function<void(const Contained&)>> on_undo_insert;
};

template <typename KeyType, typename ContainerGetter>
struct RefPtr {
  template <typename U, typename T>
  class Impl {
   public:
    static bool IsValidPre(U* obj, const KeyType& t) {
      return typename ContainerGetter::template Impl<U>()(*obj).Count(t);
    }
    static bool IsValidPost(U* obj, const KeyType& t) { return true; }
    static auto New(U* obj, const KeyType& t) {
      auto& cnt = typename ContainerGetter::template Impl<U>()(*obj);
      // TODO(veluca): add some more checks (such as that keytype is the same as
      // the returned container's key type, and/or that they look at the same
      // member), if that does not result in cycles.
      return &cnt.Get(t);
    }
    using type =
        const typename ContainerGetter::template Impl<U>::type::Contained*;
  };
};

template <typename KeyType, typename ContainerGetter>
struct ConstrainedOwnerPtr {
  template <typename U, typename T>
  class Impl {
   public:
    template <typename X>
    static bool IsValidPre(U* obj, const X& t) {
      return true;
    }
    static bool IsValidPost(U* obj, const KeyType& t) {
      return typename ContainerGetter::template Impl<U>()(*obj).Count(t);
    }
    template <typename... Args>
    static auto New(U* obj, Args... args) {
      return std::make_unique<T>(nullptr, nullptr, obj, std::move(args)...);
    }
    using type = std::unique_ptr<T>;
  };
};

}  // namespace detail

template <template <typename> class... Args>
class ContainerGetter;

template <template <typename> class Arg, template <typename> class... Args>
class ContainerGetter<Arg, Args...> {
 public:
  template <typename T>
  class Impl {
    using parent = typename member<T, Arg>::type;
    using parent_impl =
        typename ContainerGetter<Args...>::template Impl<parent>;

   public:
    using type = typename parent_impl::type;
    const type& operator()(const T& t) const {
      const parent& tmp = member<T, Arg>().ConstGet(t);
      return parent_impl()(tmp);
    }
  };
};

template <>
class ContainerGetter<> {
 public:
  template <typename T>
  class Impl {
   public:
    const T& operator()(const T& t) const { return t; }
    using type = T;
  };
};

namespace detail {
template <typename U, template <typename> class T,
          template <typename> class Key>
class BaseContainerSetup;

template <typename U, template <typename> class T,
          template <typename> class Key, typename ContainerGetter>
class BaseSubsetSetup;

template <typename U, template <typename> class T,
          template <typename> class Key, typename ContainerGetter>
class BaseConstrainedSetSetup;

}  // namespace detail

template <typename U, template <typename> class T,
          template <typename> class Key, typename ContainerGetter>
using Subset =
    detail::BaseContainer<detail::BaseSubsetSetup, U, T, Key, ContainerGetter>;

template <typename U, template <typename> class T,
          template <typename> class Key>
using Container = detail::BaseContainer<detail::BaseContainerSetup, U, T, Key>;

template <typename U, template <typename> class T,
          template <typename> class Key, typename ContainerGetter>
using ConstrainedSet = detail::BaseContainer<detail::BaseConstrainedSetSetup, U,
                                             T, Key, ContainerGetter>;

namespace detail {
template <typename U, template <typename> class T,
          template <typename> class Key>
class BaseContainerSetup {
 public:
  using Self = Container<U, T, Key>;
  using Contained = T<Self>;
  using ContainedRef = T<Self>&;
  using Inner = ::db::Value<Self, Contained>;
  using Key_t = Key<Contained>;
  using ParentType = U;
  using Ptr = OwnerPtr<Self, Inner>;
  static const constexpr bool kRequiresDir = true;
};

template <typename U, template <typename> class T,
          template <typename> class Key, typename ContainerGetter>
class BaseSubsetSetup {
 public:
  using Self = Subset<U, T, Key, ContainerGetter>;
  using Contained = T<Container<U, T, Key>>;
  using ContainedRef = const T<Container<U, T, Key>>&;
  using Inner = ::db::Value<Subset<U, T, Key, ContainerGetter>, Contained>;
  using Key_t = Key<Contained>;
  using KeyType = typename Key_t::inner_type;
  using ParentType = U;
  using Ptr = typename detail::RefPtr<KeyType, ContainerGetter>::template Impl<
      Subset<U, T, Key, ContainerGetter>, Inner>;
  static const constexpr bool kRequiresDir = false;
};

template <typename U, template <typename> class T,
          template <typename> class Key, typename ContainerGetter_>
class BaseConstrainedSetSetup {
 public:
  using ContainerGetter = ContainerGetter_;
  using Self = ConstrainedSet<U, T, Key, ContainerGetter>;
  using Contained = T<Self>;
  using ContainedRef = T<Self>&;
  using Inner = ::db::Value<Self, Contained>;
  using Key_t = Key<Contained>;
  using KeyType = typename Key_t::inner_type;
  using ParentType = U;
  using Ptr = typename detail::ConstrainedOwnerPtr<
      KeyType, ContainerGetter>::template Impl<Self, Inner>;
  using OtherContainer = typename ContainerGetter::template Impl<Self>::type;
  using SiblingType = typename OtherContainer::Contained;
  static const constexpr bool kRequiresDir = true;
  const typename OtherContainer::Contained& Sibling(const KeyType& v) const {
    return typename ContainerGetter::template Impl<Self>()(
               static_cast<const Self&>(*this))
        .Get(v);
  }
};
}  // namespace detail

}  // namespace db
