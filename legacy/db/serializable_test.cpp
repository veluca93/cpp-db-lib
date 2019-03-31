#include "db/serializable.hpp"
#include <unordered_map>
#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace db {
using testing::Eq;
using testing::Ne;

namespace {
// Base tests.
DECLARE_MEMBER(std::string, prova);
DECLARE_MEMBER(int, num);
DECLARE_MEMBER(std::vector<int>, test);

using V = db::MainData<prova_m, num_m, test_m>;

TEST(Serializable, TestConstructor) {
  std::vector<int> t{1, 2, 3};
  V v(V::Builder("ciao", 3, t));
  EXPECT_THAT(*v.test, Eq(t));
  EXPECT_THAT(*v.prova, Eq("ciao"));
  EXPECT_THAT(*v.num, Eq(3));
}

TEST(Serializable, TestDeserialize) {
  json j = R"({"test": [1], "prova": "i", "num": 3})"_json;
  auto vp = V::FromJson(nullptr, "", nullptr, j);
  EXPECT_THAT(*vp->test, Eq(std::vector<int>{1}));
  EXPECT_THAT(*vp->prova, Eq("i"));
  EXPECT_THAT(*vp->num, Eq(3));
}

TEST(Serializable, TestRoundTrip) {
  V v(V::Builder("ciao", 3, std::vector<int>{1, 2, 3}));
  json j = v.Serialize();
  auto vp = V::FromJson(nullptr, "", nullptr, j);
  EXPECT_TRUE(v == *vp);
}

// Nested data structures
DECLARE_MEMBER((std::unordered_map<std::string, int>), mp);
DECLARE_MEMBER(std::vector<int>, vec);
template <typename T>
using Nested = db::Data<T, prova_m>;
DECLARE_MEMBER(Nested<T>, data);

using Vp = db::MainData<mp_m, vec_m, data_m>;

TEST(Serializable, TestRoundNested) {
  Vp v(Vp::Builder(std::unordered_map<std::string, int>{{"ciao", 3}},
                   std::vector<int>{1, 3},
                   Vp::data_t::Builder(std::string("ciao"))));
  json j = v.Serialize();
  auto vp = Vp::FromJson(nullptr, "", nullptr, j);
  EXPECT_TRUE(v == *vp);
}

// Commit/rollback

TEST(Serializable, TestEditData) {
  V v(V::Builder("ciao", 3, std::vector<int>{1, 2, 3}));
  auto edit = v.Edit();
  *edit.num = 4;
  EXPECT_THAT(*v.num, Eq(3));
  edit.Commit();
  EXPECT_THAT(*v.num, Eq(4));
};

TEST(Serializable, TestEditDataTwice) {
  V v(V::Builder("ciao", 3, std::vector<int>{1, 2, 3}));
  auto edit = v.Edit();
  *edit.num = 4;
  EXPECT_THAT(*v.num, Eq(3));
  edit.Commit();
  EXPECT_THAT(*v.num, Eq(4));

  auto edit2 = v.Edit();
  *edit2.num = 5;
  EXPECT_THAT(*v.num, Eq(4));
  edit2.Commit();
  EXPECT_THAT(*v.num, Eq(5));
};

TEST(Serializable, TestEditDataNested) {
  Vp v(Vp::Builder(std::unordered_map<std::string, int>{{"ciao", 3}},
                   std::vector<int>{1, 3},
                   Vp::data_t::Builder(std::string("ciao"))));
  auto edit = v.Edit();
  edit.mp->emplace("test", 4);
  EXPECT_THAT(v.mp->count("test"), Eq(0));
  EXPECT_TRUE(edit.Commit());
  EXPECT_THAT(v.mp->count("test"), Eq(1));
  EXPECT_THAT(v.mp->at("test"), Eq(4));
};

TEST(Serializable, TestEditDataNestedReplace) {
  Vp v(Vp::Builder(std::unordered_map<std::string, int>{{"ciao", 3}},
                   std::vector<int>{1, 3},
                   Vp::data_t::Builder(std::string("ciao"))));
  auto edit = v.Edit();
  (*edit.mp)["ciao"] = 4;
  EXPECT_THAT(v.mp->at("ciao"), Eq(3));
  EXPECT_TRUE(edit.Commit());
  EXPECT_THAT(v.mp->count("ciao"), Eq(1));
  EXPECT_THAT(v.mp->at("ciao"), Eq(4));
};

TEST(Serializable, TestCommitRollback) {
  V v(V::Builder("ciao", 3, std::vector<int>{1, 2, 3}));
  auto edit = v.Edit(/*autocommit=*/true);
  *edit.num = 4;
  EXPECT_THAT(*v.num, Eq(3));
  EXPECT_TRUE(edit.Commit());
  EXPECT_THAT(*v.num, Eq(4));
  edit.Rollback();
  EXPECT_THAT(*v.num, Eq(3));
};

TEST(Serializable, TestEditDataNestedData) {
  Vp v(Vp::Builder(std::unordered_map<std::string, int>{{"ciao", 3}},
                   std::vector<int>{1, 3},
                   Vp::data_t::Builder(std::string("ciao"))));
  auto edit = v.Edit();
  *edit.data.prova = "test";
  EXPECT_THAT(v.data.prova, Eq("ciao"));
  EXPECT_TRUE(edit.Commit());
  EXPECT_THAT(v.data.prova, Eq("test"));
};

TEST(Serializable, TestAutocommit) {
  V v(V::Builder("ciao", 3, std::vector<int>{1, 2, 3}));
  {
    auto edit = v.Edit(/*autocommit=*/true);
    *edit.num = 4;
    EXPECT_THAT(*v.num, Eq(3));
  }
  EXPECT_THAT(*v.num, Eq(4));
};

TEST(Serializable, TestAutocommitRollback) {
  V v(V::Builder("ciao", 3, std::vector<int>{1, 2, 3}));
  {
    auto edit = v.Edit(/*autocommit=*/true);
    *edit.num = 4;
    EXPECT_THAT(*v.num, Eq(3));
    edit.Rollback();
  }
  EXPECT_THAT(*v.num, Eq(3));
};

// Commit callbacks
TEST(Serializable, TestCommitCallbackValue) {
  V v(V::Builder("ciao", 3, std::vector<int>{1, 2, 3}));
  int t = 0;
  v.num.OnChange(
      [&t](int o, int n) {
        EXPECT_THAT(n, Eq(4));
        EXPECT_THAT(o, Eq(3));
        t++;
        return true;
      },
      [](int o, int n) { EXPECT_TRUE(false); });
  auto edit = v.Edit();
  *edit.num = 4;
  EXPECT_THAT(*v.num, Eq(3));
  EXPECT_THAT(t, Eq(0));
  EXPECT_TRUE(edit.Commit());
  EXPECT_THAT(*v.num, Eq(4));
  EXPECT_THAT(t, Eq(1));
}

TEST(Serializable, TestRollbackCallbackValue) {
  V v(V::Builder("ciao", 3, std::vector<int>{1, 2, 3}));
  int t = 0;
  int t2 = 0;
  v.num.OnChange(
      [&t](int o, int n) {
        EXPECT_THAT(n, Eq(4));
        EXPECT_THAT(o, Eq(3));
        t++;
        return true;
      },
      [&t2](int o, int n) {
        EXPECT_THAT(n, Eq(4));
        EXPECT_THAT(o, Eq(3));
        t2++;
      });
  v.num.OnChange(
      [&t](int o, int n) {
        EXPECT_THAT(n, Eq(4));
        EXPECT_THAT(o, Eq(3));
        t++;
        return false;
      },
      [](int o, int n) { EXPECT_TRUE(false); });
  auto edit = v.Edit();
  *edit.num = 4;
  EXPECT_THAT(*v.num, Eq(3));
  EXPECT_THAT(t, Eq(0));
  EXPECT_FALSE(edit.Commit());
  EXPECT_THAT(*v.num, Eq(3));
  EXPECT_THAT(t, Eq(2));
  EXPECT_THAT(t2, Eq(1));
}

TEST(Serializable, TestRollbackCallbackValueDifferentFields) {
  V v(V::Builder("ciao", 3, std::vector<int>{1, 2, 3}));
  int t = 0;
  int t2 = 0;
  v.num.OnChange(
      [&t](int o, int n) {
        EXPECT_THAT(n, Eq(4));
        EXPECT_THAT(o, Eq(3));
        t++;
        return true;
      },
      [&t2](int o, int n) {
        EXPECT_THAT(n, Eq(4));
        EXPECT_THAT(o, Eq(3));
        t2++;
      });
  v.test.OnChange(
      [&t](const std::vector<int>& o, const std::vector<int>& n) {
        t++;
        return false;
      },
      [](const std::vector<int>& o, const std::vector<int>& n) {
        EXPECT_TRUE(false);
      });
  auto edit = v.Edit();
  *edit.num = 4;
  edit.test->push_back(5);
  EXPECT_THAT(*v.num, Eq(3));
  EXPECT_THAT(t, Eq(0));
  EXPECT_FALSE(edit.Commit());
  EXPECT_THAT(*v.num, Eq(3));
  EXPECT_THAT(t, Eq(2));
  EXPECT_THAT(t2, Eq(1));
}

TEST(Serializable, TestRollbackCallbackData) {
  V v(V::Builder("ciao", 3, std::vector<int>{1, 2, 3}));
  int t = 0;
  int t2 = 0;
  v.OnChange(
      [&t]() {
        t++;
        return true;
      },
      [&t2]() { t2++; });
  v.OnChange(
      [&t]() {
        t++;
        return false;
      },
      []() { EXPECT_TRUE(false); });
  auto edit = v.Edit();
  *edit.num = 4;
  EXPECT_THAT(*v.num, Eq(3));
  EXPECT_THAT(t, Eq(0));
  EXPECT_FALSE(edit.Commit());
  EXPECT_THAT(*v.num, Eq(3));
  EXPECT_THAT(t, Eq(2));
  EXPECT_THAT(t2, Eq(1));
}

TEST(Serializable, TestRollbackCallbackValueExc) {
  V v(V::Builder("ciao", 3, std::vector<int>{1, 2, 3}));
  int t = 0;
  int t2 = 0;
  v.num.OnChange(
      [&t](int o, int n) {
        EXPECT_THAT(n, Eq(4));
        EXPECT_THAT(o, Eq(3));
        t++;
        return true;
      },
      [&t2](int o, int n) {
        EXPECT_THAT(n, Eq(4));
        EXPECT_THAT(o, Eq(3));
        t2++;
      });
  v.num.OnChange(
      [&t](int o, int n) {
        EXPECT_THAT(n, Eq(4));
        EXPECT_THAT(o, Eq(3));
        t++;
        throw std::runtime_error("exc");
        return true;
      },
      [](int o, int n) { EXPECT_TRUE(false); });
  auto edit = v.Edit();
  *edit.num = 4;
  EXPECT_THAT(*v.num, Eq(3));
  EXPECT_THAT(t, Eq(0));
  EXPECT_THROW(edit.Commit(), std::runtime_error);
  EXPECT_THAT(*v.num, Eq(3));
  EXPECT_THAT(t, Eq(2));
  EXPECT_THAT(t2, Eq(1));
}

TEST(Serializable, TestRollbackCallbackValueExcDifferentFields) {
  V v(V::Builder("ciao", 3, std::vector<int>{1, 2, 3}));
  int t = 0;
  int t2 = 0;
  v.num.OnChange(
      [&t](int o, int n) {
        EXPECT_THAT(n, Eq(4));
        EXPECT_THAT(o, Eq(3));
        t++;
        return true;
      },
      [&t2](int o, int n) {
        EXPECT_THAT(n, Eq(4));
        EXPECT_THAT(o, Eq(3));
        t2++;
      });
  v.test.OnChange(
      [&t](const std::vector<int>& o, const std::vector<int>& n) {
        t++;
        throw std::runtime_error("exc");
        return true;
      },
      [](const std::vector<int>& o, const std::vector<int>& n) {
        EXPECT_TRUE(false);
      });
  auto edit = v.Edit();
  *edit.num = 4;
  edit.test->push_back(5);
  EXPECT_THAT(*v.num, Eq(3));
  EXPECT_THAT(t, Eq(0));
  EXPECT_THROW(edit.Commit(), std::runtime_error);
  EXPECT_THAT(*v.num, Eq(3));
  EXPECT_THAT(t, Eq(2));
  EXPECT_THAT(t2, Eq(1));
}

TEST(Serializable, TestRollbackCallbackDataExc) {
  V v(V::Builder("ciao", 3, std::vector<int>{1, 2, 3}));
  int t = 0;
  int t2 = 0;
  v.OnChange(
      [&t]() {
        t++;
        return true;
      },
      [&t2]() { t2++; });
  v.OnChange(
      [&t]() {
        t++;
        throw std::runtime_error("exc");
        return true;
      },
      []() { EXPECT_TRUE(false); });
  auto edit = v.Edit();
  *edit.num = 4;
  EXPECT_THAT(*v.num, Eq(3));
  EXPECT_THAT(t, Eq(0));
  EXPECT_THROW(edit.Commit(), std::runtime_error);
  EXPECT_THAT(*v.num, Eq(3));
  EXPECT_THAT(t, Eq(2));
  EXPECT_THAT(t2, Eq(1));
}

// Failure conditions
TEST(Serializable, TestDoubleCommit) {
  V v(V::Builder("ciao", 3, std::vector<int>{1, 2, 3}));
  auto edit = v.Edit();
  edit.Commit();
  EXPECT_THROW(edit.Commit(), kj::Exception);
}

TEST(Serializable, TestUndoNoCommit) {
  V v(V::Builder("ciao", 3, std::vector<int>{1, 2, 3}));
  auto edit = v.Edit();
  EXPECT_THROW(edit.UndoCommit(), kj::Exception);
}

TEST(Serializable, TestRollbackThenCommit) {
  V v(V::Builder("ciao", 3, std::vector<int>{1, 2, 3}));
  auto edit = v.Edit();
  edit.Rollback();
  EXPECT_THROW(edit.Commit(), kj::Exception);
}

TEST(Serializable, TestDoubleRollback) {
  V v(V::Builder("ciao", 3, std::vector<int>{1, 2, 3}));
  auto edit = v.Edit();
  edit.Rollback();
  EXPECT_THROW(edit.Rollback(), kj::Exception);
}

TEST(SerializableDeathTest, TestRollbackCallbackThrowsValue) {
  V v(V::Builder("ciao", 3, std::vector<int>{1, 2, 3}));
  v.num.OnChange([](int a, int b) { return true; },
                 [](int a, int b) { throw std::runtime_error("exception"); });
  v.num.OnChange([](int a, int b) { return false; }, [](int a, int b) {});
  auto edit = v.Edit();
  *edit.num = 4;
  EXPECT_THAT(*v.num, Eq(3));
  EXPECT_DEATH(edit.Commit(), "terminate.*exception");
}

TEST(SerializableDeathTest, TestRollbackCallbackThrowsData) {
  V v(V::Builder("ciao", 3, std::vector<int>{1, 2, 3}));
  v.OnChange([]() { return true; },
             []() { throw std::runtime_error("exception"); });
  v.OnChange([]() { return false; }, []() {});
  auto edit = v.Edit();
  *edit.num = 4;
  EXPECT_THAT(*v.num, Eq(3));
  EXPECT_DEATH(edit.Commit(), "terminate.*exception");
}

// On-disk write

TEST(Serializable, TestWriteOnCreate) {
  auto dir = kj::newInMemoryDirectory(kj::nullClock());
  V v(V::Builder("ciao", 3, std::vector<int>{1, 2, 3})
          .SetDir(dir->clone())
          .SetField("stuff"));
  auto vp = V::Load(dir->clone(), "stuff", nullptr);
  EXPECT_TRUE(v == *vp);
};

TEST(Serializable, TestNoSubdir1) {
  auto dir = kj::newInMemoryDirectory(kj::nullClock());
  V v(V::Builder("ciao", 3, std::vector<int>{1, 2, 3}).SetDir(dir->clone()));
  auto vp = V::Load(dir->clone(), nullptr, nullptr);
  EXPECT_TRUE(v == *vp);
};

TEST(Serializable, TestNoSubdir2) {
  auto dir = kj::newInMemoryDirectory(kj::nullClock());
  V v(V::Builder("ciao", 3, std::vector<int>{1, 2, 3})
          .SetDir(dir->clone())
          .SetField(""));
  auto vp = V::Load(dir->clone(), "", nullptr);
  EXPECT_TRUE(v == *vp);
};

TEST(Serializable, TestWriteOnCommit) {
  auto dir = kj::newInMemoryDirectory(kj::nullClock());
  V v(V::Builder("ciao", 3, std::vector<int>{1, 2, 3})
          .SetDir(dir->clone())
          .SetField("stuff"));
  auto edit = v.Edit();
  *edit.num = 4;
  edit.Commit();
  auto vp = V::Load(dir->clone(), "stuff", nullptr);
  EXPECT_TRUE(v == *vp);
};

// Parent pointer
TEST(Serializable, TestParent) {
  Vp v(Vp::Builder(std::unordered_map<std::string, int>{{"ciao", 3}},
                   std::vector<int>{1, 3},
                   Vp::data_t::Builder(std::string("ciao"))));
  EXPECT_THAT(v.Parent(), Eq(nullptr));
  EXPECT_THAT(v.data.Parent(), Ne(nullptr));
};

TEST(Serializable, TestParentAfterEdit) {
  Vp v(Vp::Builder(std::unordered_map<std::string, int>{{"ciao", 3}},
                   std::vector<int>{1, 3},
                   Vp::data_t::Builder(std::string("ciao"))));
  EXPECT_THAT(v.Parent(), Eq(nullptr));
  EXPECT_THAT(v.data.Parent(), Ne(nullptr));
  auto edit = v.Edit();
  edit.vec->push_back(4);
  EXPECT_TRUE(edit.Commit());
  EXPECT_THAT(v.data.Parent()->vec->size(), Eq(3));
};

TEST(Serializable, TestParentAfterMoveAndEdit) {
  auto v = std::make_unique<Vp>(Vp::Builder(
      std::unordered_map<std::string, int>{{"ciao", 3}}, std::vector<int>{1, 3},
      Vp::data_t::Builder(std::string("ciao"))));
  auto vp = std::move(v);
  auto edit = vp->Edit();
  edit.vec->push_back(4);
  EXPECT_TRUE(edit.Commit());
  EXPECT_THAT(vp->data.Parent()->vec->size(), Eq(3));
};

// Subfield edit

TEST(Serializable, TestSubCommit) {
  Vp v(Vp::Builder(std::unordered_map<std::string, int>{{"ciao", 3}},
                   std::vector<int>{1, 3},
                   Vp::data_t::Builder(std::string("ciao"))));
  EXPECT_THAT(v.Parent(), Eq(nullptr));
  EXPECT_THAT(v.data.Parent(), Ne(nullptr));
  v.OnChange([]() { return false; }, []() {});
  auto edit = v.data.Edit();
  *edit.prova = "test";
  EXPECT_TRUE(edit.Commit());
  EXPECT_THAT(*v.data.prova, Eq("test"));
};

}  // namespace
}  // namespace db
