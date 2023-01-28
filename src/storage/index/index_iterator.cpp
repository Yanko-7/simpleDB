/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(int pos, B_PLUS_TREE_LEAF_PAGE_TYPE *page, BufferPoolManager *buffer_pool_manager) {
  buffer_pool_manager_ = buffer_pool_manager;
  page_ = page;
  pos_ = pos;
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() {
  buffer_pool_manager_ = nullptr;
  page_ = nullptr;
  buffer_pool_manager_ = nullptr;
}  // NOLINT

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool {
  return static_cast<bool>(page_->GetSize() - 1 == pos_ && page_->GetNextPageId() == INVALID_PAGE_ID);
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::GetPage(page_id_t page_id) -> B_PLUS_TREE_LEAF_PAGE_TYPE * {
  if (page_id == INVALID_PAGE_ID) {
    return nullptr;
  }
  auto buffer_page = buffer_pool_manager_->FetchPage(page_id);
  return reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(buffer_page->GetData());
}
INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & { return page_->GetIdx(pos_); }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  if (pos_ == page_->GetSize() - 1) {
    page_id_t next_id = page_->GetNextPageId();
    buffer_pool_manager_->UnpinPage(page_->GetPageId(), false);
    page_ = GetPage(next_id);
    pos_ = 0;
  } else {
    pos_++;
  }
  return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
