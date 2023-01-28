//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include "common/exception.h"
#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  page_table_ = new ExtendibleHashTable<page_id_t, frame_id_t>(bucket_size_);
  replacer_ = new LRUKReplacer(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete page_table_;
  delete replacer_;
}

auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t frame_id;
  if (!GetFrame(&frame_id)) {
    return nullptr;
  }

  *page_id = AllocatePage();
  auto *page = GetPage(frame_id);

  // recor the info
  replacer_->SetEvictable(frame_id, false);
  pages_set_.insert(*page_id);
  page_table_->Insert(*page_id, frame_id);
  page->page_id_ = *page_id;
  page->pin_count_++;
  page->ResetMemory();
  page->is_dirty_ = false;
  replacer_->RecordAccess(frame_id);

  return page;
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t frame_id;
  if (!page_table_->Find(page_id, frame_id)) {
    if (!GetFrame(&frame_id)) {
      return nullptr;
    }

    auto *page = GetPage(frame_id);

    // recor the info
    pages_set_.insert(page_id);
    page_table_->Insert(page_id, frame_id);
    page->page_id_ = page_id;
    page->pin_count_++;
    page->is_dirty_ = false;
    page->ResetMemory();
    replacer_->SetEvictable(frame_id, false);
    replacer_->RecordAccess(frame_id);
    disk_manager_->ReadPage(page_id, page->data_);
    return page;
  }
  replacer_->SetEvictable(frame_id, false);
  GetPage(frame_id)->pin_count_++;
  replacer_->RecordAccess(frame_id);
  return GetPage(frame_id);
}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t frame_id;
  if (!page_table_->Find(page_id, frame_id)) {
    return false;
  }
  auto *page = GetPage(frame_id);
  if (page->GetPinCount() == 0) {
    return false;
  }
  // need lock
  page->pin_count_--;
  if (is_dirty) {
    page->is_dirty_ = is_dirty;
  }
  if (page->GetPinCount() == 0) {
    replacer_->SetEvictable(frame_id, true);
  }
  return true;
}

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t frame_id;
  if (!page_table_->Find(page_id, frame_id)) {
    return false;
  }
  auto *page = GetPage(frame_id);
  disk_manager_->WritePage(page_id, page->data_);
  page->is_dirty_ = false;
  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  std::scoped_lock<std::mutex> lock(latch_);
  for (auto page_id : pages_set_) {
    frame_id_t frame_id;
    if (!page_table_->Find(page_id, frame_id)) {
      continue;
    }
    auto *page = GetPage(frame_id);
    disk_manager_->WritePage(page_id, page->data_);
    page->is_dirty_ = false;
  }
}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t frame_id;
  if (!page_table_->Find(page_id, frame_id)) {
    return true;
  }
  auto *page = GetPage(frame_id);
  if (page->GetPinCount() > 0) {
    return false;
  }
  page->ResetMemory();
  page->is_dirty_ = false;
  page->pin_count_ = 0;
  free_list_.push_back(frame_id);

  page_table_->Remove(page_id);
  replacer_->Remove(frame_id);

  pages_set_.erase(page_id);
  page->page_id_ = INVALID_PAGE_ID;
  DeallocatePage(page_id);
  return true;
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManagerInstance::GetFrame(frame_id_t *frame_id) -> bool {
  if (!free_list_.empty()) {
    *frame_id = free_list_.back();
    free_list_.pop_back();
    return true;
  }
  if (!replacer_->Evict(frame_id)) {
    return false;
  }
  auto *page = GetPage(*frame_id);
  if (page->IsDirty()) {
    disk_manager_->WritePage(page->GetPageId(), page->data_);
    page->is_dirty_ = false;
  }
  page->ResetMemory();
  page_table_->Remove(page->GetPageId());
  pages_set_.erase(page->GetPageId());
  page->pin_count_ = 0;
  page->page_id_ = INVALID_PAGE_ID;
  return true;
}
auto BufferPoolManagerInstance::GetPage(frame_id_t frame_id) -> Page * { return &GetPages()[frame_id]; }

}  // namespace bustub
