//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cassert>
#include <cstdlib>
#include <functional>
#include <list>
#include <utility>

#include "container/hash/extendible_hash_table.h"
#include "storage/page/page.h"

namespace bustub {

template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size)
    : global_depth_(0), bucket_size_(bucket_size), num_buckets_(1) {
  dir_.push_back(std::make_shared<Bucket>(bucket_size_, 0));
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {
  int mask = (1 << global_depth_) - 1;
  return std::hash<K>()(key) & mask;
}

template <typename K, typename V>
inline auto ExtendibleHashTable<K, V>::Incredir() -> void {
  global_depth_++;
  dir_.resize(dir_.size() * 2);
  // 11 111
  int mask = dir_.size() / 2;
  for (int i = mask; i < static_cast<int>(dir_.size()); i++) {
    dir_[i] = dir_[i - mask];
  }
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepth() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetGlobalDepthInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepthInternal() const -> int {
  return global_depth_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepth(int dir_index) const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetLocalDepthInternal(dir_index);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepthInternal(int dir_index) const -> int {
  return dir_[dir_index]->GetDepth();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBuckets() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetNumBucketsInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBucketsInternal() const -> int {
  return num_buckets_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  int idx = static_cast<int>(IndexOf(key));
  return dir_[idx]->Find(key, value);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  int idx = IndexOf(key);
  return dir_[idx]->Remove(key);
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  std::scoped_lock<std::mutex> lock(latch_);
  int idx = static_cast<int>(IndexOf(key));
  while (!dir_[idx]->Insert(key, value)) {
    if (GetLocalDepthInternal(idx) == GetGlobalDepthInternal()) {
      Incredir();
    }
    dir_[idx]->IncrementDepth();
    RedistributeBucket(dir_[idx], idx);
    idx = static_cast<int>(IndexOf(key));
  }
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::RedistributeBucket(std::shared_ptr<Bucket> bucket, int idx) -> void {
  num_buckets_++;
  std::shared_ptr<Bucket> &tmp = bucket;
  int mask = ((1 << bucket->GetDepth()) - 1);
  int one = idx & mask;
  int two = one ^ (1 << (bucket->GetDepth() - 1));
  auto p1 = std::make_shared<Bucket>(bucket_size_, tmp->GetDepth());
  auto p2 = std::make_shared<Bucket>(bucket_size_, tmp->GetDepth());
  int off = bucket->GetDepth();
  for (int i = 0; i < (1 << (GetGlobalDepthInternal() - off)); i++) {
    dir_[i << off | one] = p1;
    dir_[i << off | two] = p2;
  }
  // dir_[one] = std::make_shared<Bucket>(bucket_size_, tmp->GetDepth());
  // dir_[two] = std::make_shared<Bucket>(bucket_size_, tmp->GetDepth());
  for (auto it = tmp->GetItems().begin(); it != tmp->GetItems().end(); it++) {
    if ((static_cast<int>(IndexOf(it->first)) & mask) == one) {
      p1->Insert(it->first, it->second);
    } else {
      p2->Insert(it->first, it->second);
    }
  }
}

//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  for (auto it = GetItems().begin(); it != GetItems().end(); it++) {
    if (it->first == key) {
      value = it->second;
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  auto &list = GetItems();
  int siz = static_cast<int>(list.size());
  list.remove_if([&](std::pair<K, V> &tmp) { return tmp.first == key; });
  return siz != static_cast<int>(list.size());
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  if (IsFull()) {
    return false;
  }
  auto &x = GetItems();
  for (auto it = GetItems().begin(); it != GetItems().end(); it++) {
    if (it->first == key) {
      it->second = value;
      return true;
    }
  }
  x.insert(x.end(), std::make_pair(key, value));
  return true;
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub
