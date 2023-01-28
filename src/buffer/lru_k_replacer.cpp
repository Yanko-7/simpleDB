//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  if (replacers_.empty()) {
    return false;
  }
  bool flag = false;
  for (auto x : replacers_) {
    if (!flag || GetLastTime(x) < GetLastTime(*frame_id)) {
      *frame_id = x;
      flag = true;
    } else if (GetLastTime(x) == GetLastTime(*frame_id)) {
      if (timestamp_[x].front() < timestamp_[*frame_id].front()) {
        *frame_id = x;
      }
    }
  }
  replacers_.erase(*frame_id);
  timestamp_.erase(*frame_id);
  return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  BUSTUB_ASSERT(frame_id <= static_cast<int>(replacer_size_) - 1, true);
  Addstamp(frame_id);
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::scoped_lock<std::mutex> lock(latch_);
  BUSTUB_ASSERT(frame_id <= static_cast<int>(replacer_size_) - 1, true);
  if (set_evictable) {
    if (timestamp_.find(frame_id) != timestamp_.end()) {
      replacers_.insert(frame_id);
    }
  } else if (replacers_.find(frame_id) != replacers_.end()) {
    replacers_.erase(frame_id);
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  if (timestamp_.find(frame_id) != timestamp_.end()) {
    BUSTUB_ASSERT(replacers_.find(frame_id) != replacers_.end(), true);
  }
  if (timestamp_.find(frame_id) == timestamp_.end() || replacers_.find(frame_id) == replacers_.end()) {
    return;
  }
  replacers_.erase(frame_id);
  timestamp_.erase(frame_id);
}

auto LRUKReplacer::Size() -> size_t {
  std::scoped_lock<std::mutex> lock(latch_);
  return replacers_.size();
}

auto LRUKReplacer::GetLastTime(frame_id_t frame_id) -> size_t {
  if (timestamp_[frame_id].size() < k_) {
    return INF;
  }
  return timestamp_[frame_id].front();
}

auto LRUKReplacer::Creattime() -> size_t { return ++current_timestamp_; }

auto LRUKReplacer::Addstamp(frame_id_t frame_id) -> void {
  timestamp_[frame_id].push(Creattime());
  if (timestamp_[frame_id].size() > k_) {
    timestamp_[frame_id].pop();
  }
}

}  // namespace bustub
