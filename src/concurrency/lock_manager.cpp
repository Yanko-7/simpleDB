//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"

#include "common/config.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"

namespace bustub {

void LockManager::CheckIsolationLevel(Transaction *txn, LockMode lock_mode) {
  switch (txn->GetIsolationLevel()) {
    case IsolationLevel::REPEATABLE_READ:
      if (txn->GetState() == TransactionState::SHRINKING) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
      }
      break;
    case IsolationLevel::READ_COMMITTED:
      if (txn->GetState() == TransactionState::SHRINKING && lock_mode != LockMode::INTENTION_SHARED &&
          lock_mode != LockMode::SHARED) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
      }
      break;
    case IsolationLevel::READ_UNCOMMITTED:
      if (txn->GetState() != TransactionState::GROWING ||
          (lock_mode != LockMode::EXCLUSIVE && lock_mode != LockMode::INTENTION_EXCLUSIVE)) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
      }
      break;
  }
}

void LockManager::CheckSuitable(Transaction *txn, LockMode lock_mode, LockRequest *req, txn_id_t upgrading) {
  if (upgrading != INVALID_TXN_ID) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
  }
  //
  std::cout<<(int)lock_mode<<' '<<(int)req->lock_mode_<<std::endl;
  if (req->lock_mode_ == LockMode::EXCLUSIVE) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
  }
  switch (lock_mode) {
    case LockMode::SHARED:
      if (req->lock_mode_ != LockMode::INTENTION_SHARED) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
      }
      break;
    case LockMode::EXCLUSIVE:
      break;
    case LockMode::INTENTION_SHARED:
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
      break;
    case LockMode::INTENTION_EXCLUSIVE:
      if (req->lock_mode_ != LockMode::INTENTION_SHARED) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
      }
      break;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      break;
  }
}

void LockManager::EraseFromTxn(Transaction *txn, LockMode lock_mode, table_oid_t oid) {
  switch (lock_mode) {
    case LockMode::SHARED:
      txn->GetSharedTableLockSet()->erase(oid);
      break;
    case LockMode::EXCLUSIVE:
      txn->GetExclusiveTableLockSet()->erase(oid);
      break;
    case LockMode::INTENTION_SHARED:
      txn->GetIntentionSharedTableLockSet()->erase(oid);
      break;
    case LockMode::INTENTION_EXCLUSIVE:
      txn->GetIntentionExclusiveTableLockSet()->erase(oid);
      break;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      txn->GetSharedIntentionExclusiveTableLockSet()->erase(oid);
      break;
  }
}
void LockManager::EraseFromTxn(Transaction *txn, LockMode lock_mode, table_oid_t oid, RID rid) {
  switch (lock_mode) {
    case LockMode::SHARED:
      (*txn->GetSharedRowLockSet())[oid].erase(rid);
      txn->GetSharedLockSet()->erase(rid);
      // LOG_DEBUG("txn:%d erasesrow",txn->GetTransactionId());
      break;
    case LockMode::EXCLUSIVE:
      (*txn->GetExclusiveRowLockSet())[oid].erase(rid);
      txn->GetExclusiveLockSet()->erase(rid);
      // LOG_DEBUG("txn:%d erasexrow",txn->GetTransactionId());
      break;
    default:
      break;
  }
}
void LockManager::InsertToTxn(Transaction *txn, LockMode lock_mode, table_oid_t table_id) {
  switch (lock_mode) {
    case LockMode::SHARED:
      txn->GetSharedTableLockSet()->insert(table_id);
      break;
    case LockMode::EXCLUSIVE:
      txn->GetExclusiveTableLockSet()->insert(table_id);
      break;
    case LockMode::INTENTION_SHARED:
      txn->GetIntentionSharedTableLockSet()->insert(table_id);
      break;
    case LockMode::INTENTION_EXCLUSIVE:
      txn->GetIntentionExclusiveTableLockSet()->insert(table_id);
      break;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      txn->GetSharedIntentionExclusiveTableLockSet()->insert(table_id);
      break;
  }
}
void LockManager::InsertToTxn(Transaction *txn, LockMode lock_mode, table_oid_t table_id, RID rid) {
  switch (lock_mode) {
    case LockMode::SHARED:
      (*txn->GetSharedRowLockSet())[table_id].insert(rid);
      (*txn->GetSharedLockSet()).insert(rid);
      break;
    case LockMode::EXCLUSIVE:
      (*txn->GetExclusiveRowLockSet())[table_id].insert(rid);
      (*txn->GetExclusiveLockSet()).insert(rid);
      break;
    default:
      break;
  }
}
auto LockManager::LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
  if (txn->GetState() == TransactionState::ABORTED || txn->GetState() == TransactionState::COMMITTED) {
    // throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_LOCK_NOT_PRESENT);
    return false;
  }

  // checkIsolationLevel
  CheckIsolationLevel(txn, lock_mode);

  table_lock_map_latch_.lock();
  if (table_lock_map_.find(oid) == table_lock_map_.end()) {
    table_lock_map_[oid] = std::make_shared<LockRequestQueue>();
  }
  auto &table_request_que = table_lock_map_[oid];
  table_lock_map_latch_.unlock();

  std::unique_lock<std::mutex> lock(table_request_que->latch_);

  // check update
  for (auto req = table_request_que->request_queue_.begin(); req != table_request_que->request_queue_.end(); req++) {
    if (req.operator*()->txn_id_ == txn->GetTransactionId()) {
      if (req.operator*()->lock_mode_ == lock_mode) {
        return true;
      }
      // check and update
      CheckSuitable(txn, lock_mode, req->get(), table_request_que->upgrading_);
      // delete
      EraseFromTxn(txn, req.operator*()->lock_mode_, req.operator*()->oid_);
      // delete (*req);
      table_request_que->request_queue_.erase(req);
      table_request_que->upgrading_ = txn->GetTransactionId();
      auto request = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid);
      bool has_insert = false;
      for (auto iter = table_request_que->request_queue_.begin(); iter != table_request_que->request_queue_.end();
           iter++) {
        if (!(*iter)->granted_) {
          table_request_que->request_queue_.insert(iter, request);
          has_insert = true;
          break;
        }
      }
      if (!has_insert) {
        table_request_que->request_queue_.insert(table_request_que->request_queue_.end(), request);
      }
      return GetLock(table_request_que, txn, lock_mode, oid, lock);
    }
  }
  // grant
  auto request = new LockRequest(txn->GetTransactionId(), lock_mode, oid);
  table_request_que->request_queue_.emplace_back(request);
  return GetLock(table_request_que, txn, lock_mode, oid, lock);
}

auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool {
  // judge has map
  table_lock_map_latch_.lock();
  if (table_lock_map_.find(oid) == table_lock_map_.end()) {
    table_lock_map_[oid] = std::make_shared<LockRequestQueue>();
  }
  auto &table_request_que = table_lock_map_[oid];
  table_lock_map_latch_.unlock();
  // judge no has rowlock
  bool has = false;
  for (auto &x : *txn->GetExclusiveRowLockSet()) {
    if (!x.second.empty() && x.first == oid) {
      has = true;
    }
  }
  for (auto &x : *txn->GetSharedRowLockSet()) {
    if (!x.second.empty() && x.first == oid) {
      has = true;
    }
  }
  if (has) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS);
  }
  // find a key
  bool flag = false;
  std::list<std::shared_ptr<LockRequest>>::iterator pos;
  std::unique_lock<std::mutex> lock(table_request_que->latch_);
  for (auto req = table_request_que->request_queue_.begin(); req != table_request_que->request_queue_.end(); req++) {
    if ((*req)->granted_ && (*req)->txn_id_ == txn->GetTransactionId()) {
      pos = req;
      flag = true;
      break;
    }
  }
  // if no exist
  if (!flag) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }

  // set the status ,you can ensure the suit lock
  if (txn->GetState() != TransactionState::COMMITTED && txn->GetState() != TransactionState::ABORTED) {
    switch (txn->GetIsolationLevel()) {
      case IsolationLevel::REPEATABLE_READ:
        if ((*pos)->lock_mode_ == LockMode::EXCLUSIVE || (*pos)->lock_mode_ == LockMode::SHARED) {
          txn->SetState(TransactionState::SHRINKING);
        }
        break;
      case IsolationLevel::READ_COMMITTED:
      case IsolationLevel::READ_UNCOMMITTED:
        if ((*pos)->lock_mode_ == LockMode::EXCLUSIVE) {
          txn->SetState(TransactionState::SHRINKING);
        }
        break;
    }
  }
  EraseFromTxn(txn, (*pos)->lock_mode_, (*pos)->oid_);
  table_request_que->request_queue_.erase(pos);
  table_request_que->cv_.notify_all();
  return true;
}

auto LockManager::LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  if (lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE || lock_mode == LockMode::INTENTION_SHARED ||
      lock_mode == LockMode::INTENTION_EXCLUSIVE) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_INTENTION_LOCK_ON_ROW);
    return false;
  }
  if (txn->GetState() == TransactionState::ABORTED || txn->GetState() == TransactionState::COMMITTED) {
    // throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_LOCK_NOT_PRESENT);
    return false;
  }
  // check
  CheckIsolationLevel(txn, lock_mode);

  table_lock_map_latch_.lock();
  if (table_lock_map_.find(oid) == table_lock_map_.end()) {
    table_lock_map_[oid] = std::make_shared<LockRequestQueue>();
  }
  auto &table_request_que = table_lock_map_[oid];
  table_lock_map_latch_.unlock();

  table_request_que->latch_.lock();
  bool flag = false;
  // check table has lock
  if (lock_mode == LockMode::EXCLUSIVE) {
    for (const auto &req : table_request_que->request_queue_) {
      if (req->txn_id_ == txn->GetTransactionId() &&
          (req->lock_mode_ == LockMode::EXCLUSIVE || req->lock_mode_ == LockMode::INTENTION_EXCLUSIVE ||
           req->lock_mode_ == LockMode::SHARED_INTENTION_EXCLUSIVE)) {
        flag = true;
        break;
      }
    }
  }
  if (lock_mode == LockMode::SHARED) {
    for (const auto &req : table_request_que->request_queue_) {
      if (req->txn_id_ == txn->GetTransactionId()) {
        flag = true;
        break;
      }
    }
  }
  if (!flag) {
    table_request_que->latch_.unlock();
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_LOCK_NOT_PRESENT);
  }
  table_request_que->latch_.unlock();
  // check update
  row_lock_map_latch_.lock();
  if (row_lock_map_.find(rid) == row_lock_map_.end()) {
    row_lock_map_[rid] = std::make_shared<LockRequestQueue>();
  }
  auto &row_request_que = row_lock_map_[rid];
  row_lock_map_latch_.unlock();

  std::unique_lock<std::mutex> lock(row_request_que->latch_);
  for (auto req = row_request_que->request_queue_.begin(); req != row_request_que->request_queue_.end(); req++) {
    if (req.operator*()->txn_id_ == txn->GetTransactionId()) {
      if (req.operator*()->lock_mode_ == lock_mode) {
        return true;
      }
      // check and update
      CheckSuitable(txn, lock_mode, req->get(), row_request_que->upgrading_);
      // delete
      EraseFromTxn(txn, req.operator*()->lock_mode_, req.operator*()->oid_, req.operator*()->rid_);
      row_request_que->request_queue_.erase(req);
      row_request_que->upgrading_ = txn->GetTransactionId();
      auto request = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid, rid);
      bool has_insert = false;
      for (auto iter = row_request_que->request_queue_.begin(); iter != row_request_que->request_queue_.end(); iter++) {
        if (!(*iter)->granted_) {
          row_request_que->request_queue_.insert(iter, request);
          has_insert = true;
          break;
        }
      }
      if (!has_insert) {
        row_request_que->request_queue_.insert(row_request_que->request_queue_.end(), request);
      }
      return GetLock(row_request_que, txn, lock_mode, oid, rid, lock);
    }
  }
  // grant
  auto request = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid, rid);
  row_request_que->request_queue_.push_back(request);
  return GetLock(row_request_que, txn, lock_mode, oid, rid, lock);
}

auto LockManager::UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid) -> bool {
  row_lock_map_latch_.lock();
  if (row_lock_map_.find(rid) == row_lock_map_.end()) {
    row_lock_map_[rid] = std::make_shared<LockRequestQueue>();
  }
  auto &row_request_que = row_lock_map_[rid];
  row_lock_map_latch_.unlock();
  bool flag = false;
  std::list<std::shared_ptr<LockRequest>>::iterator pos;
  std::unique_lock<std::mutex> lock(row_request_que->latch_);
  for (auto req = row_request_que->request_queue_.begin(); req != row_request_que->request_queue_.end(); req++) {
    if ((*req)->granted_ && (*req)->txn_id_ == txn->GetTransactionId() && rid == (*req)->rid_) {
      pos = req;
      flag = true;
      break;
    }
  }
  if (!flag) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }

  if (txn->GetState() != TransactionState::COMMITTED && txn->GetState() != TransactionState::ABORTED) {
    switch (txn->GetIsolationLevel()) {
      case IsolationLevel::REPEATABLE_READ:
        if ((*pos)->lock_mode_ == LockMode::EXCLUSIVE || (*pos)->lock_mode_ == LockMode::SHARED) {
          txn->SetState(TransactionState::SHRINKING);
        }
        break;
      case IsolationLevel::READ_COMMITTED:
      case IsolationLevel::READ_UNCOMMITTED:
        if ((*pos)->lock_mode_ == LockMode::EXCLUSIVE) {
          txn->SetState(TransactionState::SHRINKING);
        }
        break;
    }
  }
  EraseFromTxn(txn, (*pos)->lock_mode_, (*pos)->oid_, (*pos)->rid_);
  row_request_que->request_queue_.erase(pos);
  row_request_que->cv_.notify_all();
  return true;
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {
  std::unique_lock<std::mutex> lock(waits_for_latch_);
  for (auto x : waits_for_[t1]) {
    if (x == t2) {
      return;
    }
  }
  waits_for_[t1].push_back(t2);
}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {
  std::unique_lock<std::mutex> lock(waits_for_latch_);
  for (auto x = waits_for_[t1].begin(); x != waits_for_[t1].end(); x++) {
    if (*x == t2) {
      waits_for_[t1].erase(x);
      break;
    }
  }
}

auto LockManager::DFS(txn_id_t u, txn_id_t maxnid) -> txn_id_t {
  sett_.insert(u);
  auto &vec = waits_for_[u];
  sort(vec.begin(), vec.end());
  for (auto v : vec) {
    if (sett_.find(v) != sett_.end()) {
      return maxnid;
    }
    if (book_.find(v) != book_.end()) {
      continue;
    }
    txn_id_t tmp = DFS(v, std::max(maxnid, v));
    if (tmp != INVALID_TXN_ID) {
      return tmp;
    }
  }
  return INVALID_TXN_ID;
}

auto LockManager::HasCycle(txn_id_t *txn_id) -> bool {
  std::unique_lock<std::mutex> lock(waits_for_latch_);
  std::vector<std::pair<txn_id_t, std::vector<txn_id_t>>> vec;
  vec.reserve(waits_for_.size());
  for (auto &x : waits_for_) {
    vec.emplace_back(x);
  }
  std::sort(vec.begin(), vec.end());
  return std::any_of(vec.begin(), vec.end(), [&](const std::pair<txn_id_t, std::vector<txn_id_t>> &x) {
    if (book_.find(x.first) == book_.end()) {
      sett_.clear();
      txn_id_t tmp = DFS(x.first, x.first);
      if (tmp != INVALID_TXN_ID) {
        *txn_id = tmp;
        return true;
      }
    }
    return false;
  });
}

void LockManager::CreateGraph() {
  waits_for_.clear();
  table_lock_map_latch_.lock();
  for (const auto &x : table_lock_map_) {
    (*x.second).latch_.lock();
    auto &que = (*x.second).request_queue_;
    for (const auto &u : que) {
      for (const auto &v : que) {
        if (u->txn_id_ != v->txn_id_ && !u->granted_ && v->granted_ &&
            !g_[static_cast<int>(u->lock_mode_)][static_cast<int>(v->lock_mode_)]) {
          AddEdge(u->txn_id_, v->txn_id_);
        }
      }
    }
    (*x.second).latch_.unlock();
  }
  table_lock_map_latch_.unlock();
  row_lock_map_latch_.lock();
  for (const auto &x : row_lock_map_) {
    (*x.second).latch_.lock();
    auto &que = (*x.second).request_queue_;
    for (const auto &u : que) {
      for (const auto &v : que) {
        if (u->txn_id_ != v->txn_id_ && !u->granted_ && v->granted_ &&
            !g_[static_cast<int>(u->lock_mode_)][static_cast<int>(v->lock_mode_)]) {
          AddEdge(u->txn_id_, v->txn_id_);
        }
      }
    }
    (*x.second).latch_.unlock();
  }
  row_lock_map_latch_.unlock();
}

auto LockManager::GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>> {
  std::unique_lock<std::mutex> lock(waits_for_latch_);
  std::vector<std::pair<txn_id_t, txn_id_t>> edges(0);
  for (auto u : waits_for_) {
    auto &vec = u.second;
    std::sort(vec.begin(), vec.end());
    for (auto v : vec) {
      edges.emplace_back(u.first, v);
    }
  }
  return edges;
}

void LockManager::Inform(Transaction *txn) {
  txn->SetState(TransactionState::ABORTED);
  std::shared_ptr<LockRequestQueue> inform_que;
  table_lock_map_latch_.lock();
  bool flag = false;
  for (auto &x : table_lock_map_) {
    x.second->latch_.lock();
    for (const auto &v : x.second->request_queue_) {
      if (v->txn_id_ == txn->GetTransactionId() && !v->granted_) {
        inform_que = x.second;
        flag = true;
        break;
      }
    }
    x.second->latch_.unlock();
    if (flag) {
      break;
    }
  }
  table_lock_map_latch_.unlock();
  if (!flag) {
    row_lock_map_latch_.lock();
    for (auto &x : row_lock_map_) {
      x.second->latch_.lock();
      for (const auto &v : x.second->request_queue_) {
        if (v->txn_id_ == txn->GetTransactionId() && !v->granted_) {
          inform_que = x.second;
          flag = true;
          break;
        }
      }
      x.second->latch_.unlock();
      if (flag) {
        break;
      }
    }
    row_lock_map_latch_.unlock();
  }
  inform_que->cv_.notify_all();
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {  // TODO(students): detect deadlock
      txn_id_t txn_id = INVALID_TXN_ID;
      sett_.clear();
      book_.clear();
      if (!enable_cycle_detection_) {
        break;
      }
      CreateGraph();
      while (HasCycle(&txn_id)) {
        Transaction *txn = TransactionManager::GetTransaction(txn_id);
        if (enable_cycle_detection_) {
          book_.insert(txn_id);
          Inform(txn);
        }
      }
    }
  }
}

auto LockManager::GetLock(std::shared_ptr<LockRequestQueue> que, Transaction *txn, LockMode lock_mode,
                          table_oid_t table_id, std::unique_lock<std::mutex> &lock) -> bool {
  auto grant = [&]() {
    // LOG_DEBUG("txnid: %d,status:%d",txn->GetTransactionId(),(int)txn->GetState());
    if (que->upgrading_ != INVALID_TXN_ID && que->upgrading_ != txn->GetTransactionId()) {
      return false;
    }
    for (auto iter1 = que->request_queue_.begin(); iter1 != que->request_queue_.end(); iter1++) {
      if (!iter1.operator*()->granted_ && iter1.operator*()->txn_id_ == txn->GetTransactionId()) {
        for (auto iter2 = que->request_queue_.begin(); iter2 != iter1; iter2++) {
          for (auto iter3 = que->request_queue_.begin(); iter3 != iter2; iter3++) {
            if (!g_[static_cast<int>(iter2.operator*()->lock_mode_)][static_cast<int>(iter3.operator*()->lock_mode_)]) {
              return false;
            }
          }
        }
        for (auto iter2 = que->request_queue_.begin(); iter2 != iter1; iter2++) {
          if (!g_[static_cast<int>(iter2.operator*()->lock_mode_)][static_cast<int>(iter1.operator*()->lock_mode_)]) {
            return false;
          }
        }
        return true;
      }
    }
    return false;
  };
  while (!grant()) {
    que->cv_.wait(lock);
    if (txn->GetState() == TransactionState::ABORTED) {
      for (auto iter = que->request_queue_.begin(); iter != que->request_queue_.end(); iter++) {
        if (!iter.operator*()->granted_ && iter.operator*()->txn_id_ == txn->GetTransactionId()) {
          que->request_queue_.erase(iter);
          if (que->upgrading_ == txn->GetTransactionId()) {
            que->upgrading_ = INVALID_TXN_ID;
          }
          que->cv_.notify_all();
          break;
        }
      }
      return false;
    }
  }
  for (auto &x : que->request_queue_) {
    if (!x->granted_ && x->txn_id_ == txn->GetTransactionId()) {
      x->granted_ = true;
      InsertToTxn(txn, lock_mode, table_id);
      if (que->upgrading_ == txn->GetTransactionId()) {
        que->upgrading_ = INVALID_TXN_ID;
      }
      break;
    }
  }
  return true;
}
auto LockManager::GetLock(std::shared_ptr<LockRequestQueue> que, Transaction *txn, LockManager::LockMode lock_mode,
                          table_oid_t table_id, RID rid, std::unique_lock<std::mutex> &lock) -> bool {
  auto grant = [&]() {
    // LOG_DEBUG("txnid: %d,status:%d",txn->GetTransactionId(),(int)txn->GetState());
    if (que->upgrading_ != INVALID_TXN_ID && que->upgrading_ != txn->GetTransactionId()) {
      return false;
    }
    for (auto iter1 = que->request_queue_.begin(); iter1 != que->request_queue_.end(); iter1++) {
      if (!iter1.operator*()->granted_ && iter1.operator*()->txn_id_ == txn->GetTransactionId()) {
        for (auto iter2 = que->request_queue_.begin(); iter2 != iter1; iter2++) {
          for (auto iter3 = que->request_queue_.begin(); iter3 != iter2; iter3++) {
            if (!g_[static_cast<int>(iter2.operator*()->lock_mode_)][static_cast<int>(iter3.operator*()->lock_mode_)]) {
              return false;
            }
          }
        }
        for (auto iter2 = que->request_queue_.begin(); iter2 != iter1; iter2++) {
          if (!g_[static_cast<int>(iter2.operator*()->lock_mode_)][static_cast<int>(iter1.operator*()->lock_mode_)]) {
            return false;
          }
        }
        return true;
      }
    }
    return false;
  };
  while (!grant()) {
    que->cv_.wait(lock);
    // LOG_DEBUG("txn:%d",txn->GetTransactionId());
    if (txn->GetState() == TransactionState::ABORTED) {
      for (auto iter = que->request_queue_.begin(); iter != que->request_queue_.end(); iter++) {
        if (!iter.operator*()->granted_ && iter.operator*()->txn_id_ == txn->GetTransactionId()) {
          que->request_queue_.erase(iter);
          if (que->upgrading_ == txn->GetTransactionId()) {
            que->upgrading_ = INVALID_TXN_ID;
          }
          que->cv_.notify_all();
          break;
        }
      }
      return false;
    }
  }
  for (auto &x : que->request_queue_) {
    if (!x->granted_ && x->txn_id_ == txn->GetTransactionId()) {
      x->granted_ = true;
      InsertToTxn(txn, lock_mode, table_id, rid);
      if (que->upgrading_ == txn->GetTransactionId()) {
        que->upgrading_ = INVALID_TXN_ID;
      }
      break;
    }
  }
  // LOG_DEBUG("txnid:%d",txn->GetTransactionId());
  return true;
}

}  // namespace bustub
