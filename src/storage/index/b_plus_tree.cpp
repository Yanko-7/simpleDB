#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/header_page.h"

namespace bustub {
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindIndex(const KeyType &key, InternalPage *page_id) -> int {
  if (page_id->GetSize() == 1) {
    return 0;
  }
  int l = 1;
  int r = page_id->GetSize() - 1;
  while (r > l) {
    int mid = (r + l + 1) / 2;
    if (comparator_(page_id->KeyAt(mid), key) <= 0) {
      l = mid;
    } else {
      r = mid - 1;
    }
  }
  if (l == 1) {
    if (comparator_(page_id->KeyAt(1), key) > 0) {
      return 0;
    }
  }
  return l;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindIndex(const KeyType &key, LeafPage *page_id) -> int {
  if(page_id->GetSize() == 0){ return -1; }
  if (page_id->GetSize() == 1) {
    if (comparator_(page_id->KeyAt(0), key) > 0) {
      return -1;
    }
    return 0;
  }
  int l = 1;
  int r = page_id->GetSize() - 1;
  while (r > l) {
    int mid = (r + l + 1) / 2;
    if (comparator_(page_id->KeyAt(mid), key) <= 0) {
      l = mid;
    } else {
      r = mid - 1;
    }
  }
  if (l == 1) {
    if (comparator_(page_id->KeyAt(0), key) > 0) {
      return -1;
    }
    if (comparator_(page_id->KeyAt(1), key) > 0) {
      return 0;
    }
  }
  return l;
}
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetInternalPage(BPlusTreePage *page) -> InternalPage * {
  return static_cast<InternalPage *>(page);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetLeafPage(BPlusTreePage *page) -> LeafPage * { return static_cast<LeafPage *>(page); }

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RootLock(LatchType type) {
  if (type == LatchType::INSERT || type == LatchType::DELETE) {
    root_rwlatch_.WLock();
  } else {
    root_rwlatch_.RLock();
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RootUnLock(LatchType type) {
  if (type == LatchType::INSERT || type == LatchType::DELETE) {
    root_rwlatch_.WUnlock();
  } else {
    root_rwlatch_.RUnlock();
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Lock(Page *buffer_page, LatchType type) {
  if (type == LatchType::INSERT || type == LatchType::DELETE) {
    buffer_page->WLatch();
  } else {
    buffer_page->RLatch();
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ULock(Page *buffer_page, LatchType type) {
  if (type == LatchType::INSERT || type == LatchType::DELETE) {
    buffer_page->WUnlatch();
  } else {
    buffer_page->RUnlatch();
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Judge(BPlusTreePage *page, LatchType type) -> bool {
  if (type == LatchType::INSERT) {
    if (page->IsLeafPage()) {
      return page->GetSize() + 1 < page->GetMaxSize();
    }
    return page->GetSize() < page->GetMaxSize();
  }
  if (type == LatchType::DELETE) {
    return page->GetSize() - 1 >= page->GetMinSize();
  }
  return true;
}
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ClearTxPage(Transaction *transaction, LatchType type) {
  while (!transaction->GetPageSet()->empty()) {
    auto que = transaction->GetPageSet()->front();
    transaction->GetPageSet()->pop_front();
    bool is_root = reinterpret_cast<BPlusTreePage *>(que->GetData())->IsRootPage();
    ULock(que, type);
    buffer_pool_manager_->UnpinPage(que->GetPageId(), true);
    if (is_root) {
      RootUnLock(type);
    }
  }
}
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::OptimGetLeaf(const KeyType &key, LatchType type) -> Page * {
  auto last_buffer_page = buffer_pool_manager_->FetchPage(GetRootPageId());
  Lock(last_buffer_page, LatchType::QUERRY);
  auto page = reinterpret_cast<BPlusTreePage *>(last_buffer_page->GetData());
  page->SetParentPageId(INVALID_PAGE_ID);
  if (page->IsLeafPage()) {
    ULock(last_buffer_page, LatchType::QUERRY);
    buffer_pool_manager_->UnpinPage(last_buffer_page->GetPageId(), true);
    RootUnLock(LatchType::QUERRY);
    return nullptr;
  }
  while (true) {
    if (page->IsLeafPage()) {
      if (!Judge(page, type)) {
        bool is_root = page->IsRootPage();
        ULock(last_buffer_page, type);
        buffer_pool_manager_->UnpinPage(last_buffer_page->GetPageId(), true);
        if (is_root) {
          RootUnLock(LatchType::QUERRY);
        }
        return nullptr;
      }
      break;
    }
    auto last_page = static_cast<InternalPage *>(page);
    int idx = FindIndex(key, last_page);
    page_id_t next_page_id = last_page->ValueAt(idx);
    auto next_buffer_page = buffer_pool_manager_->FetchPage(next_page_id);
    page = reinterpret_cast<BPlusTreePage *>(next_buffer_page->GetData());
    page->SetParentPageId(last_page->GetPageId());
    if (page->IsLeafPage()) {
      Lock(next_buffer_page, type);
    } else {
      Lock(next_buffer_page, LatchType::QUERRY);
    }
    bool is_root = last_page->IsRootPage();
    ULock(last_buffer_page, LatchType::QUERRY);
    buffer_pool_manager_->UnpinPage(last_buffer_page->GetPageId(), true);
    if (is_root) {
      RootUnLock(LatchType::QUERRY);
    }
    last_buffer_page = next_buffer_page;
  }
  return last_buffer_page;
}
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetLeaf(const KeyType &key, Transaction *transaction, LatchType type)
    -> Page * {  // return the last page and the transaction has the last page
  auto last_buffer_page = buffer_pool_manager_->FetchPage(GetRootPageId());
  Lock(last_buffer_page, type);
  if (type != LatchType::QUERRY) {
    transaction->AddIntoPageSet(last_buffer_page);
  }
  auto page = reinterpret_cast<BPlusTreePage *>(last_buffer_page->GetData());
  page->SetParentPageId(INVALID_PAGE_ID);
  while (true) {
    if (page->IsLeafPage()) {
      break;
    }
    auto last_page = static_cast<InternalPage *>(page);
    int idx = FindIndex(key, last_page);
    page_id_t next_page_id = last_page->ValueAt(idx);
    auto next_buffer_page = buffer_pool_manager_->FetchPage(next_page_id);
    Lock(next_buffer_page, type);
    page = reinterpret_cast<BPlusTreePage *>(next_buffer_page->GetData());
    page->SetParentPageId(last_page->GetPageId());
    if (type == LatchType::QUERRY) {
      bool is_root = last_page->IsRootPage();
      ULock(last_buffer_page, type);
      buffer_pool_manager_->UnpinPage(last_buffer_page->GetPageId(), true);
      if (is_root) {
        RootUnLock(type);
      }
      last_buffer_page = next_buffer_page;
      continue;
    }
    if (Judge(page, type)) {  // if safe clear the que
      ClearTxPage(transaction, type);
    }
    last_buffer_page = next_buffer_page;
    // put in now_page
    transaction->AddIntoPageSet(next_buffer_page);
  }
  return last_buffer_page;
}
/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool { return root_page_id_ == INVALID_PAGE_ID; }
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) -> bool {
  RootLock(LatchType::QUERRY);
  auto buffer_page = GetLeaf(key, transaction, LatchType::QUERRY);  // buffer page
  bool flag = false;
  auto leaf_page = reinterpret_cast<LeafPage *>(buffer_page->GetData());  // data_page
  int idx = FindIndex(key, leaf_page);
  if (idx != -1 && comparator_(key, leaf_page->KeyAt(idx)) == 0) {
    flag = true;
    result->push_back(leaf_page->ValueAt(idx));
  }
  bool is_root = leaf_page->IsRootPage();
  ULock(buffer_page, LatchType::QUERRY);
  buffer_pool_manager_->UnpinPage(buffer_page->GetPageId(), true);
  if (is_root) {
    RootUnLock(LatchType::QUERRY);
  }
  return flag;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertInParent(page_id_t other_page_id, const KeyType &Key, Transaction *transaction) {
  auto parent_buffer_page = transaction->GetPageSet()->back();
  transaction->GetPageSet()->pop_back();
  auto parent_page = reinterpret_cast<InternalPage *>(parent_buffer_page->GetData());
  auto parent_page_id = parent_page->GetPageId();
  if (parent_page->GetSize() == parent_page->GetMaxSize()) {
    page_id_t new_page_id;
    auto new_buffer_page = buffer_pool_manager_->NewPage(&new_page_id);
    auto new_page = reinterpret_cast<InternalPage *>(new_buffer_page->GetData());
    new_page->Init(new_page_id, parent_page->GetParentPageId(), internal_max_size_);
    std::vector<std::pair<KeyType, page_id_t>> tmp(parent_page->GetArray(),
                                                   parent_page->GetArray() + parent_page->GetSize());
    bool flag = false;
    for (int i = 1; i < static_cast<int>(tmp.size()); i++) {
      if (comparator_(Key, tmp[i].first) <= 0) {
        tmp.insert(tmp.begin() + i, std::make_pair(Key, other_page_id));
        flag = true;
        break;
      }
    }
    if (!flag) {
      tmp.insert(tmp.end(), std::make_pair(Key, other_page_id));
    }
    int len = tmp.size();
    parent_page->SetSize((len - 1) / 2 + 1);
    new_page->SetSize(len - ((len - 1) / 2 + 1));
    for (int i = 0; i <= (len - 1) / 2; i++) {
      parent_page->SetKeyAt(i, tmp[i].first);
      parent_page->SetValueAt(i, tmp[i].second);
    }
    int dis = (len - 1) / 2 + 1;
    for (int i = dis; i < len; i++) {
      new_page->SetKeyAt(i - dis, tmp[i].first);
      new_page->SetValueAt(i - dis, tmp[i].second);
    }
    KeyType tmp_key = new_page->KeyAt(0);

    if (parent_page->IsRootPage()) {
      page_id_t root_id;
      auto root_buffer_page = buffer_pool_manager_->NewPage(&root_id);
      Lock(root_buffer_page, LatchType::INSERT);
      auto root_page = reinterpret_cast<InternalPage *>(root_buffer_page->GetData());
      root_page->Init(root_id, INVALID_PAGE_ID, internal_max_size_);
      root_page->SetSize(1);
      root_page->SetValueAt(0, parent_page_id);
      root_page_id_ = root_id;
      UpdateRootPageId(true);
      transaction->AddIntoPageSet(root_buffer_page);
    }
    ULock(parent_buffer_page, LatchType::INSERT);
    buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(new_page->GetPageId(), true);

    InsertInParent(new_page_id, tmp_key, transaction);
    return;
  }
  int idx = FindIndex(Key, parent_page);
  parent_page->Insert(idx + 1, Key, other_page_id);
  bool is_root = parent_page->IsRootPage();
  ULock(parent_buffer_page, LatchType::INSERT);
  buffer_pool_manager_->UnpinPage(parent_page_id, true);
  if (is_root) {
    RootUnLock(LatchType::INSERT);
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) -> bool {
  // LOG_DEBUG("InsertRootLock");
  if (IsEmpty()) {
    RootLock(LatchType::INSERT);
  } else {
    RootLock(LatchType::QUERRY);
  }
  if (IsEmpty()) {
    page_id_t root_id;
    auto buffer_page = buffer_pool_manager_->NewPage(&root_id);
    auto page = reinterpret_cast<LeafPage *>(buffer_page->GetData());
    page->Init(root_id, INVALID_PAGE_ID, leaf_max_size_);
    page->SetSize(1);
    // insert entry
    page->SetKeyAt(0, key);
    page->SetValueAt(0, value);
    root_page_id_ = root_id;
    // exit
    UpdateRootPageId(true);
    buffer_pool_manager_->UnpinPage(root_id, true);
    RootUnLock(LatchType::INSERT);
    return true;
  }
  auto leaf_buffer_page = OptimGetLeaf(key, LatchType::INSERT);
  if (leaf_buffer_page == nullptr) {
    RootLock(LatchType::INSERT);
    leaf_buffer_page = GetLeaf(key, transaction, LatchType::INSERT);
    transaction->GetPageSet()->pop_back();
  }
  auto leaf_page = reinterpret_cast<LeafPage *>(leaf_buffer_page->GetData());  // data_page
  int idx = FindIndex(key, leaf_page);
  if (idx != -1 && comparator_(leaf_page->KeyAt(idx), key) == 0) {
    ClearTxPage(transaction, LatchType::INSERT);
    bool is_root = leaf_page->IsRootPage();
    ULock(leaf_buffer_page, LatchType::INSERT);
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
    if (is_root) {
      RootUnLock(LatchType::INSERT);
    }
    return false;
  }

  leaf_page->Insert(idx + 1, key, value);
  if (leaf_page->GetSize() == leaf_page->GetMaxSize()) {
    // leaf split
    page_id_t other_page_id;
    auto other_buffer_page = buffer_pool_manager_->NewPage(&other_page_id);
    auto other_page = reinterpret_cast<LeafPage *>(other_buffer_page->GetData());
    other_page->Init(other_page_id, leaf_page->GetParentPageId(), leaf_max_size_);

    // setdata
    other_page->SetSize((leaf_page->GetSize() + 1) / 2);
    int dis = leaf_page->GetSize() / 2;
    for (int i = 0; i < other_page->GetSize(); i++) {
      other_page->SetKeyAt(i, leaf_page->KeyAt(i + dis));
      other_page->SetValueAt(i, leaf_page->ValueAt(i + dis));
    }
    // set nextpageid
    other_page->SetNextPageId(leaf_page->GetNextPageId());
    leaf_page->SetNextPageId(other_page->GetPageId());
    leaf_page->SetSize(leaf_page->GetSize() / 2);
    // pushup
    page_id_t leaf_page_id = leaf_page->GetPageId();
    KeyType tmp_key = other_page->KeyAt(0);
    if (leaf_page->IsRootPage()) {
      page_id_t root_id;
      auto root_buffer_page = buffer_pool_manager_->NewPage(&root_id);
      Lock(root_buffer_page, LatchType::INSERT);
      auto root_page = reinterpret_cast<InternalPage *>(root_buffer_page->GetData());
      root_page->Init(root_id, INVALID_PAGE_ID, internal_max_size_);
      root_page->SetSize(1);
      root_page->SetValueAt(0, leaf_page_id);
      root_page_id_ = root_id;
      UpdateRootPageId(true);
      transaction->AddIntoPageSet(root_buffer_page);
    }
    ULock(leaf_buffer_page, LatchType::INSERT);
    buffer_pool_manager_->UnpinPage(other_page->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
    InsertInParent(other_page_id, tmp_key, transaction);
    return true;
  }
  // ClearTxPage(transaction, LatchType::INSERT);
  bool is_root = leaf_page->IsRootPage();
  ULock(leaf_buffer_page, LatchType::INSERT);
  buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
  if (is_root) {
    RootUnLock(LatchType::INSERT);
  }
  return true;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::DeleteFromParent(page_id_t delete_page_id, Transaction *transaction) {
  auto buffer_page = transaction->GetPageSet()->back();
  transaction->GetPageSet()->pop_back();
  auto page = reinterpret_cast<InternalPage *>(buffer_page->GetData());
  page->Delete(delete_page_id);
  if (page->IsRootPage()) {
    if (page->GetSize() == 1) {
      root_page_id_ = page->ValueAt(0);
      UpdateRootPageId(true);
      ULock(buffer_page, LatchType::DELETE);
      buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
      buffer_pool_manager_->DeletePage(page->GetPageId());
      RootUnLock(LatchType::DELETE);
      return;
    }
    ULock(buffer_page, LatchType::DELETE);
    buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
    RootUnLock(LatchType::DELETE);
    return;
  }
  if (page->GetSize() >= page->GetMinSize()) {
    ULock(buffer_page, LatchType::DELETE);
    buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
    return;
  }

  auto parent_buffer_page = transaction->GetPageSet()->back();
  auto parent_page = reinterpret_cast<InternalPage *>(parent_buffer_page->GetData());
  // choose borther node
  int idx = FindIndex(page->KeyAt(1), parent_page);
  int other_idx = -1;
  if (idx != parent_page->GetSize() - 1) {
    other_idx = idx + 1;
  } else {
    other_idx = idx - 1;
  }
  auto other_buffer_page = buffer_pool_manager_->FetchPage(parent_page->ValueAt(other_idx));
  Lock(other_buffer_page, LatchType::DELETE);
  auto other_page = reinterpret_cast<InternalPage *>(other_buffer_page->GetData());
  other_page->SetParentPageId(parent_page->GetPageId());
  // merge
  if (other_page->GetSize() + page->GetSize() <= page->GetMaxSize()) {
    if (idx > other_idx) {
      std::swap(page, other_page);
      std::swap(idx, other_idx);
    }
    int dis = page->GetSize();
    page->IncreaseSize(other_page->GetSize());
    for (int i = dis; i < page->GetSize(); i++) {
      page->SetKeyAt(i, other_page->KeyAt(i - dis));
      page->SetValueAt(i, other_page->ValueAt(i - dis));
    }
    page->SetKeyAt(dis, parent_page->KeyAt(other_idx));
    // Delete page
    ULock(buffer_page, LatchType::DELETE);
    ULock(other_buffer_page, LatchType::DELETE);
    buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(other_page->GetPageId(), true);
    buffer_pool_manager_->DeletePage(other_page->GetPageId());  // insure is the last node
    //
    DeleteFromParent(parent_page->ValueAt(other_idx), transaction);
  } else {  // get_from brother
    if (idx > other_idx) {
      page->Insert(0, other_page->KeyAt(other_page->GetSize() - 1), other_page->ValueAt(other_page->GetSize() - 1));
      page->SetKeyAt(1, parent_page->KeyAt(idx));
      parent_page->SetKeyAt(idx, page->KeyAt(0));
      other_page->IncreaseSize(-1);
    } else {
      page->Insert(page->GetSize(), other_page->KeyAt(0), other_page->ValueAt(0));
      page->SetKeyAt(page->GetSize() - 1, parent_page->KeyAt(other_idx));
      parent_page->SetKeyAt(other_idx, other_page->KeyAt(1));
      other_page->Delete(other_page->ValueAt(0));
    }
    // Delete page
    ClearTxPage(transaction, LatchType::DELETE);
    ULock(buffer_page, LatchType::DELETE);
    ULock(other_buffer_page, LatchType::DELETE);
    buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(other_page->GetPageId(), true);
  }
}
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  // LOG_DEBUG("RemoveRootLock");
  RootLock(LatchType::QUERRY);
  auto leaf_buffer_page = OptimGetLeaf(key, LatchType::DELETE);
  if (leaf_buffer_page == nullptr) {
    RootLock(LatchType::DELETE);
    leaf_buffer_page = GetLeaf(key, transaction, LatchType::DELETE);
    transaction->GetPageSet()->pop_back();
  }
  auto leaf_page = reinterpret_cast<LeafPage *>(leaf_buffer_page->GetData());
  if (!leaf_page->Delete(key, comparator_)) {
    ClearTxPage(transaction, LatchType::DELETE);
    bool is_root = leaf_page->IsRootPage();
    ULock(leaf_buffer_page, LatchType::DELETE);
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
    if (is_root) {
      RootUnLock(LatchType::DELETE);
    }
    return;
  }
  if (leaf_page->IsRootPage()) {
    ULock(leaf_buffer_page, LatchType::DELETE);
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
    RootUnLock(LatchType::DELETE);
    return;
  }

  if (leaf_page->GetSize() >= leaf_page->GetMinSize()) {
    ULock(leaf_buffer_page, LatchType::DELETE);
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
    return;
  }

  auto parent_buffer_page = transaction->GetPageSet()->back();
  auto parent_page = reinterpret_cast<InternalPage *>(parent_buffer_page->GetData());
  // choose borther node
  int idx = FindIndex(key, parent_page);
  int other_idx = -1;
  if (idx != parent_page->GetSize() - 1) {
    other_idx = idx + 1;
  } else {
    other_idx = idx - 1;
  }
  auto other_buffer_page = buffer_pool_manager_->FetchPage(parent_page->ValueAt(other_idx));
  Lock(other_buffer_page, LatchType::DELETE);
  auto other_page = reinterpret_cast<LeafPage *>(other_buffer_page->GetData());
  other_page->SetParentPageId(parent_page->GetPageId());
  // merge
  if (other_page->GetSize() + leaf_page->GetSize() < leaf_page->GetMaxSize()) {
    if (idx > other_idx) {
      std::swap(leaf_page, other_page);
      std::swap(idx, other_idx);
    }
    int dis = leaf_page->GetSize();
    leaf_page->IncreaseSize(other_page->GetSize());
    leaf_page->SetNextPageId(other_page->GetNextPageId());
    for (int i = dis; i < leaf_page->GetSize(); i++) {
      leaf_page->SetKeyAt(i, other_page->KeyAt(i - dis));
      leaf_page->SetValueAt(i, other_page->ValueAt(i - dis));
    }
    // Delete page
    ULock(leaf_buffer_page, LatchType::DELETE);
    ULock(other_buffer_page, LatchType::DELETE);
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(other_page->GetPageId(), true);
    buffer_pool_manager_->DeletePage(other_page->GetPageId());  // insure is the last node
    //
    DeleteFromParent(parent_page->ValueAt(other_idx), transaction);
  } else {  // get_from brother
    if (idx > other_idx) {
      leaf_page->Insert(0, other_page->KeyAt(other_page->GetSize() - 1),
                        other_page->ValueAt(other_page->GetSize() - 1));
      parent_page->SetKeyAt(idx, leaf_page->KeyAt(0));
      other_page->IncreaseSize(-1);
    } else {
      leaf_page->Insert(leaf_page->GetSize(), other_page->KeyAt(0), other_page->ValueAt(0));
      parent_page->SetKeyAt(other_idx, other_page->KeyAt(1));
      other_page->Delete(other_page->KeyAt(0), comparator_);
    }
    // Delete page
    ClearTxPage(transaction, LatchType::DELETE);
    ULock(leaf_buffer_page, LatchType::DELETE);
    ULock(other_buffer_page, LatchType::DELETE);
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(other_page->GetPageId(), true);
  }
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  if(GetRootPageId() == INVALID_PAGE_ID){
    return INDEXITERATOR_TYPE(0, nullptr, buffer_pool_manager_);
  }
  auto buffer_page = buffer_pool_manager_->FetchPage(GetRootPageId());
  auto page = reinterpret_cast<BPlusTreePage *>(buffer_page->GetData());
  page->SetParentPageId(INVALID_PAGE_ID);
  while (true) {
    if (page->IsLeafPage()) {
      break;
    }
    auto last_page_id = page->GetPageId();
    page_id_t page_id = static_cast<InternalPage *>(page)->ValueAt(0);
    buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
    buffer_page = buffer_pool_manager_->FetchPage(page_id);
    page = reinterpret_cast<BPlusTreePage *>(buffer_page->GetData());
    page->SetParentPageId(last_page_id);
  }
  if (page->GetSize() == 0) {
    page = nullptr;
  }
  return INDEXITERATOR_TYPE(0, static_cast<LeafPage *>(page), buffer_pool_manager_);
}
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  if(GetRootPageId() == INVALID_PAGE_ID){
    return INDEXITERATOR_TYPE(0, nullptr, buffer_pool_manager_);
  }
  auto buffer_page = buffer_pool_manager_->FetchPage(GetRootPageId());
  auto page = reinterpret_cast<BPlusTreePage *>(buffer_page->GetData());
  page->SetParentPageId(INVALID_PAGE_ID);
  while (true) {
    if (page->IsLeafPage()) {
      break;
    }
    auto new_page = static_cast<InternalPage *>(page);
    int idx = FindIndex(key, new_page);
    page_id_t page_id = new_page->ValueAt(idx);
    buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
    buffer_page = buffer_pool_manager_->FetchPage(page_id);
    page = reinterpret_cast<BPlusTreePage *>(buffer_page->GetData());
    page->SetParentPageId(new_page->GetPageId());
  }
  auto new_page = static_cast<LeafPage *>(page);
  int idx = FindIndex(key, new_page);
  return INDEXITERATOR_TYPE(idx, new_page, buffer_pool_manager_);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(0, nullptr, buffer_pool_manager_); }

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t { return root_page_id_; }

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  auto *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Draw an empty tree");
    return;
  }
  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  ToGraph(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm, out);
  out << "}" << std::endl;
  out.flush();
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  if (IsEmpty()) {
    LOG_WARN("Print an empty tree");
    return;
  }
  ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm);
}

/**
 * This method is used for debug only, You don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
