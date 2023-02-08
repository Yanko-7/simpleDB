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

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k)
    : replacer_size_(num_frames), k_(k), timestamp_(num_frames, Frameinfo(k)) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  if (nomax_replacers_.size() + max_replacers_.size() == 0) {
    return false;
  }
  if (!nomax_replacers_.empty()) {
    *frame_id = *nomax_replacers_.begin();
    for (auto x : nomax_replacers_) {
      if (timestamp_[*frame_id].Getime() > timestamp_[x].Getime()) {
        *frame_id = x;
      }
    }
    nomax_replacers_.erase(*frame_id);
  } else {
    *frame_id = *max_replacers_.begin();
    for (auto x : max_replacers_) {
      if (timestamp_[*frame_id].Getime() > timestamp_[x].Getime()) {
        *frame_id = x;
      }
    }
    max_replacers_.erase(*frame_id);
  }
  timestamp_[*frame_id].Clear();
  return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  BUSTUB_ASSERT(frame_id <= static_cast<int>(replacer_size_) - 1, true);
  Addstamp(frame_id);
}

void LRUKReplacer::DelReplacers(frame_id_t frame_id) {
  auto &x = timestamp_[frame_id];
  if (!x.IsLive()) {
    return;
  }
  if (x.IsMax()) {
    if (InMaxReplacers(frame_id)) {
      max_replacers_.erase(frame_id);
    }
  } else {
    if (InNoMaxReplacers(frame_id)) {
      nomax_replacers_.erase(frame_id);
    }
  }
}

void LRUKReplacer::AddReplacers(frame_id_t frame_id) {
  auto &x = timestamp_[frame_id];
  if (!x.IsLive()) {
    return;
  }
  if (x.IsEvicatable()) {
    if (x.IsMax()) {
      max_replacers_.insert(frame_id);
    } else {
      nomax_replacers_.insert(frame_id);
    }
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::scoped_lock<std::mutex> lock(latch_);
  BUSTUB_ASSERT(frame_id <= static_cast<int>(replacer_size_) - 1, true);
  auto &x = timestamp_[frame_id];
  if (!x.IsLive()) {
    return;
  }
  if (set_evictable) {
    x.SetEvictable(true);
    AddReplacers(frame_id);
  } else {
    x.SetEvictable(false);
    DelReplacers(frame_id);
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  if (timestamp_[frame_id].IsLive()) {
    BUSTUB_ASSERT(InNoMaxReplacers(frame_id) || InMaxReplacers(frame_id), true);
  }
  if (!timestamp_[frame_id].IsLive()) {
    return;
  }
  DelReplacers(frame_id);
  timestamp_[frame_id].Clear();
}

auto LRUKReplacer::Size() -> size_t {
  std::scoped_lock<std::mutex> lock(latch_);
  return nomax_replacers_.size() + max_replacers_.size();
}

inline auto LRUKReplacer::InNoMaxReplacers(frame_id_t frame_id) -> bool {
  return nomax_replacers_.find(frame_id) != nomax_replacers_.end();
}

inline auto LRUKReplacer::InMaxReplacers(frame_id_t frame_id) -> bool {
  return max_replacers_.find(frame_id) != max_replacers_.end();
}
auto LRUKReplacer::Creattime() -> size_t { return ++current_timestamp_; }

auto LRUKReplacer::Addstamp(frame_id_t frame_id) -> void {
  auto &x = timestamp_[frame_id];
  x.Setlive(true);
  x.Add(Creattime());
  if (x.IsMax() && InNoMaxReplacers(frame_id)) {
    nomax_replacers_.erase(frame_id);
    max_replacers_.insert(frame_id);
  }
}
}  // namespace bustub
