#include "db/container.hpp"
#include <unordered_map>
#include "db/serializable.hpp"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace db {
using testing::Eq;
namespace {
DECLARE_MEMBER(int, test);
DECLARE_MEMBER(int, test2);

template <typename T>
using Foo = Data<T, test_m, test2_m>;

template <typename T>
using Key = member<T, test_m>;

DECLARE_MEMBER((Container<T, Foo, Key>), cont);

using Info = MainData<cont_m>;

TEST(Container, TestCommit) {
  using db::placeholders::_;
  Info inf(Info::Builder(_));
  auto edit = inf.Edit();
  edit.cont.Emplace(Info::cont_t::Builder(3, 5));
  EXPECT_TRUE(edit.Commit());
  EXPECT_THAT(inf.cont.Size(), Eq(1));
  EXPECT_THAT(*inf.cont.Get(3).test2, Eq(5));
};

TEST(Container, TestInnerEdit) {
  using db::placeholders::_;
  Info inf(Info::Builder(_));
  auto edit = inf.Edit();
  edit.cont.Emplace(Info::cont_t::Builder(3, 5));
  EXPECT_TRUE(edit.Commit());
  EXPECT_THAT(inf.cont.Size(), Eq(1));

  auto edit2 = inf.Edit();
  *edit2.cont.Get(3).test2 = 6;
  EXPECT_TRUE(edit2.Commit());
  EXPECT_THAT(*inf.cont.Get(3).test2, Eq(6));
};

TEST(Container, TestDeserialize) {
  auto dir = kj::newInMemoryDirectory(kj::nullClock());
  auto cont = dir->openSubdir(kj::Path("cont"), kj::WriteMode::CREATE);
  auto a = cont->openSubdir(kj::Path("1"), kj::WriteMode::CREATE);
  auto af = a->replaceFile(kj::Path("data.json"), kj::WriteMode::CREATE);
  af->get().writeAll(R"({"test": 1, "test2": 7})");
  af->commit();
  auto b = cont->openSubdir(kj::Path("2"), kj::WriteMode::CREATE);
  auto bf = b->replaceFile(kj::Path("data.json"), kj::WriteMode::CREATE);
  bf->get().writeAll(R"({"test": 2, "test2": 8})");
  bf->commit();
  auto j = R"({"cont": [1, 2]})"_json;
  auto inf = Info::FromJson(std::move(dir), "", nullptr, j);
  EXPECT_THAT(inf->cont.Size(), Eq(2));
  EXPECT_TRUE(inf->cont.Count(1));
  EXPECT_THAT(*inf->cont.Get(1).test2, Eq(7));
  EXPECT_TRUE(inf->cont.Count(2));
  EXPECT_THAT(*inf->cont.Get(2).test2, Eq(8));
};

TEST(Container, TestRoundTrip) {
  auto dir = kj::newInMemoryDirectory(kj::nullClock());
  using db::placeholders::_;
  Info inf(Info::Builder(_).SetDir(dir->clone()));
  auto edit = inf.Edit();
  edit.cont.Emplace(Info::cont_t::Builder(3, 5));
  EXPECT_TRUE(edit.Commit());
  EXPECT_THAT(inf.cont.Size(), Eq(1));
  EXPECT_THAT(*inf.cont.Get(3).test2, Eq(5));
  auto inf2 = Info::Load(dir->clone(), "", nullptr);
  EXPECT_TRUE(inf.cont.Get(3) == inf2->cont.Get(3));
  EXPECT_TRUE(inf.cont == inf2->cont);
  EXPECT_TRUE(inf == *inf2);
};

DECLARE_MEMBER(
    (Subset<T, Foo, Key, ContainerGetter<placeholders::parent_, cont_m>>),
    sub_cont);

using InfoSub = MainData<cont_m, sub_cont_m>;

TEST(Container, TestInsertSub) {
  using db::placeholders::_;
  InfoSub inf(InfoSub::Builder(_, _));
  auto edit = inf.Edit();
  edit.cont.Emplace(InfoSub::cont_t::Builder(3, 5));
  EXPECT_TRUE(edit.Commit());
  EXPECT_THAT(inf.cont.Size(), Eq(1));
  EXPECT_THAT(*inf.cont.Get(3).test2, Eq(5));

  auto edit2 = inf.Edit();
  EXPECT_FALSE(edit2.sub_cont.Emplace(4));
  EXPECT_TRUE(edit2.sub_cont.Emplace(3));
  EXPECT_TRUE(edit2.Commit());
  EXPECT_THAT(*inf.sub_cont.Get(3).test2, Eq(5));
};

TEST(Container, TestRoundTripSub) {
  auto dir = kj::newInMemoryDirectory(kj::nullClock());
  using db::placeholders::_;
  InfoSub inf(InfoSub::Builder(_, _).SetDir(dir->clone()));
  auto edit = inf.Edit();
  edit.cont.Emplace(InfoSub::cont_t::Builder(3, 5));
  EXPECT_TRUE(edit.Commit());
  EXPECT_THAT(inf.cont.Size(), Eq(1));
  EXPECT_THAT(*inf.cont.Get(3).test2, Eq(5));

  auto edit2 = inf.Edit();
  EXPECT_TRUE(edit2.sub_cont.Emplace(3));
  EXPECT_TRUE(edit2.Commit());
  EXPECT_THAT(*inf.sub_cont.Get(3).test2, Eq(5));
  auto inf2 = InfoSub::Load(dir->clone(), "", nullptr);
  EXPECT_TRUE(inf == *inf2);
};

DECLARE_MEMBER((ConstrainedSet<T, Foo, Key,
                               ContainerGetter<placeholders::parent_, cont_m>>),
               constr_cont);

using InfoConstr = MainData<cont_m, constr_cont_m>;

TEST(Container, TestInsertConstr) {
  using db::placeholders::_;
  InfoConstr inf(InfoConstr::Builder(_, _));
  auto edit = inf.Edit();
  edit.cont.Emplace(InfoConstr::cont_t::Builder(3, 5));
  EXPECT_TRUE(edit.Commit());
  EXPECT_THAT(inf.cont.Size(), Eq(1));
  EXPECT_THAT(*inf.cont.Get(3).test2, Eq(5));

  auto edit2 = inf.Edit();
  EXPECT_FALSE(
      edit2.constr_cont.Emplace(InfoConstr::constr_cont_t::Builder(4, 3)));
  EXPECT_TRUE(
      edit2.constr_cont.Emplace(InfoConstr::constr_cont_t::Builder(3, 6)));
  EXPECT_TRUE(edit2.Commit());
  EXPECT_THAT(*inf.cont.Get(3).test2, Eq(5));
  EXPECT_THAT(*inf.constr_cont.Get(3).test2, Eq(6));
};

TEST(Container, TestRoundTripConstr) {
  auto dir = kj::newInMemoryDirectory(kj::nullClock());
  using db::placeholders::_;
  InfoConstr inf(InfoConstr::Builder(_, _).SetDir(dir->clone()));
  auto edit = inf.Edit();
  edit.cont.Emplace(InfoConstr::cont_t::Builder(3, 5));
  EXPECT_TRUE(edit.Commit());
  EXPECT_THAT(inf.cont.Size(), Eq(1));
  EXPECT_THAT(*inf.cont.Get(3).test2, Eq(5));

  auto edit2 = inf.Edit();
  EXPECT_FALSE(
      edit2.constr_cont.Emplace(InfoConstr::constr_cont_t::Builder(4, 3)));
  EXPECT_TRUE(
      edit2.constr_cont.Emplace(InfoConstr::constr_cont_t::Builder(3, 6)));
  EXPECT_TRUE(edit2.Commit());
  auto inf2 = InfoConstr::Load(dir->clone(), "", nullptr);
  EXPECT_TRUE(inf == *inf2);
};

TEST(Container, TestConstrSibling) {
  auto dir = kj::newInMemoryDirectory(kj::nullClock());
  using db::placeholders::_;
  InfoConstr inf(InfoConstr::Builder(_, _).SetDir(dir->clone()));
  auto edit = inf.Edit();
  edit.cont.Emplace(InfoConstr::cont_t::Builder(3, 5));
  EXPECT_TRUE(edit.Commit());
  EXPECT_THAT(inf.cont.Size(), Eq(1));
  EXPECT_THAT(*inf.cont.Get(3).test2, Eq(5));

  auto edit2 = inf.Edit();
  EXPECT_FALSE(
      edit2.constr_cont.Emplace(InfoConstr::constr_cont_t::Builder(4, 3)));
  EXPECT_TRUE(
      edit2.constr_cont.Emplace(InfoConstr::constr_cont_t::Builder(3, 6)));
  EXPECT_TRUE(edit2.Commit());
  EXPECT_THAT(*inf.constr_cont.Sibling(3).test2, Eq(5));
};

}  // namespace
};  // namespace db
