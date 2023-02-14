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
      if (lock_mode != LockMode::EXCLUSIVE && lock_mode != LockMode::INTENTION_EXCLUSIVE ) {
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
      txn->GetSharedTableLockSet()->erase(oid);
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
auto LockManager::LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }

  // check
  CheckIsolationLevel(txn, lock_mode);

  table_lock_map_latch_.lock();
  if (table_lock_map_.find(oid) == table_lock_map_.end()) {
    table_lock_map_[oid] = std::make_shared<LockRequestQueue>();
  }
  auto &table_lock_map_ptr = table_lock_map_[oid];
  table_lock_map_latch_.unlock();

  std::unique_lock<std::mutex> lock(table_lock_map_ptr->latch_);

  // check update
  for (auto req = table_lock_map_ptr->request_queue_.begin(); req != table_lock_map_ptr->request_queue_.end(); req++) {
    if (req.operator*()->txn_id_ == txn->GetTransactionId()) {
      if (req.operator*()->lock_mode_ == lock_mode) {
        return true;
      }
      // check and update and delete
      CheckSuitable(txn, lock_mode, *req, table_lock_map_ptr->upgrading_);
      // check
      EraseFromTxn(txn, req.operator*()->lock_mode_, oid);
      delete (*req);
      table_lock_map_ptr->request_queue_.erase(req);
      table_lock_map_ptr->upgrading_ = txn->GetTransactionId();
      break;
    }
  }
  // grant
  auto request = new LockRequest(txn->GetTransactionId(), lock_mode, oid);
  table_lock_map_ptr->request_queue_.emplace_back(request);
  lock.unlock();
  return table_lock_map_ptr->GetLock(txn, lock_mode, oid, g_);
}

auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool {
  table_lock_map_latch_.lock();
  if (table_lock_map_.find(oid) == table_lock_map_.end()) {
    table_lock_map_[oid] = std::make_shared<LockRequestQueue>();
  }
  auto &table_lock_map_ptr = table_lock_map_[oid];
  table_lock_map_latch_.unlock();
  if (!txn->GetSharedLockSet()->empty() || !txn->GetExclusiveLockSet()->empty()) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS);
  }
  bool flag = false;
  std::list<LockRequest *>::iterator pos;
  std::unique_lock<std::mutex> lock(table_lock_map_ptr->latch_);
  for (auto req = table_lock_map_ptr->request_queue_.begin(); req != table_lock_map_ptr->request_queue_.end(); req++) {
    if ((*req)->granted_ && (*req)->txn_id_ == txn->GetTransactionId()) {
      pos = req;
      flag = true;
    }
  }
  if (!flag) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }

  if (txn->GetState() != TransactionState::COMMITTED && txn->GetState() != TransactionState::ABORTED) {
    switch (txn->GetIsolationLevel()) {
      case IsolationLevel::REPEATABLE_READ:
        txn->SetState(TransactionState::SHRINKING);
        break;
      case IsolationLevel::READ_COMMITTED:
      case IsolationLevel::READ_UNCOMMITTED:
        if ((*pos)->lock_mode_ == LockMode::EXCLUSIVE) {
          txn->SetState(TransactionState::SHRINKING);
        }
        break;
    }
  }
  EraseFromTxn(txn, (*pos)->lock_mode_, oid);
  delete (*pos);
  table_lock_map_ptr->request_queue_.erase(pos);
  table_lock_map_ptr->cv_.notify_all();

  return true;
}

auto LockManager::LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  if (lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE || lock_mode == LockMode::INTENTION_SHARED ||
      lock_mode == LockMode::INTENTION_EXCLUSIVE) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_INTENTION_LOCK_ON_ROW);
  }
  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }
  // check
  CheckIsolationLevel(txn, lock_mode);

  table_lock_map_latch_.lock();
  if (table_lock_map_.find(oid) == table_lock_map_.end()) {
    table_lock_map_[oid] = std::make_shared<LockRequestQueue>();
  }
  auto &table_lock_map_ptr = table_lock_map_[oid];
  table_lock_map_latch_.unlock();

  std::unique_lock<std::mutex> lock(table_lock_map_ptr->latch_);
  bool flag = false;
  if (lock_mode == LockMode::EXCLUSIVE) {
    for (auto req : table_lock_map_ptr->request_queue_) {
      if (req->txn_id_ == txn->GetTransactionId() &&
          (req->lock_mode_ == LockMode::EXCLUSIVE || req->lock_mode_ == LockMode::INTENTION_EXCLUSIVE ||
           req->lock_mode_ == LockMode::SHARED_INTENTION_EXCLUSIVE)) {
        flag = true;
        break;
      }
    }
  }
  if (lock_mode == LockMode::SHARED) {
    for (auto req : table_lock_map_ptr->request_queue_) {
      if (req->txn_id_ == txn->GetTransactionId() &&
          (req->lock_mode_ == LockMode::INTENTION_EXCLUSIVE ||
           req->lock_mode_ == LockMode::SHARED_INTENTION_EXCLUSIVE || req->lock_mode_ == LockMode::SHARED ||
           req->lock_mode_ == LockMode::INTENTION_SHARED)) {
        flag = true;
        break;
      }
    }
  }
  if (!flag) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_LOCK_NOT_PRESENT);
  }
  // check update
  row_lock_map_latch_.lock();
  if (row_lock_map_.find(rid) == row_lock_map_.end()) {
    row_lock_map_[rid] = std::make_shared<LockRequestQueue>();
  }
  auto &row_lock_map_ptr = row_lock_map_[rid];
  row_lock_map_latch_.unlock();
  for (auto req = row_lock_map_ptr->request_queue_.begin(); req != row_lock_map_ptr->request_queue_.end(); req++) {
    if (req.operator*()->txn_id_ == txn->GetTransactionId()) {
      if (req.operator*()->lock_mode_ == lock_mode) {
        return true;
      }
      // check and update and delete
      CheckSuitable(txn, lock_mode, *req, row_lock_map_ptr->upgrading_);
      EraseFromTxn(txn, req.operator*()->lock_mode_, oid, rid);
      delete (*req);
      row_lock_map_ptr->request_queue_.erase(req);
      row_lock_map_ptr->upgrading_ = txn->GetTransactionId();
      break;
    }
  }
  // grant
  auto request = new LockRequest(txn->GetTransactionId(), lock_mode, oid, rid);
  row_lock_map_ptr->request_queue_.emplace_back(request);
  lock.unlock();
  return row_lock_map_ptr->GetLock(txn, lock_mode, oid, rid, g_);
}

auto LockManager::UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid) -> bool {
  row_lock_map_latch_.lock();
  auto &row_lock_map_ptr = row_lock_map_[rid];
  row_lock_map_latch_.unlock();
  bool flag = false;
  std::list<LockRequest *>::iterator pos;
  std::unique_lock<std::mutex> lock(row_lock_map_ptr->latch_);
  for (auto req = row_lock_map_ptr->request_queue_.begin(); req != row_lock_map_ptr->request_queue_.end(); req++) {
    if ((*req)->granted_ && (*req)->txn_id_ == txn->GetTransactionId()) {
      pos = req;
      flag = true;
    }
  }
  if (!flag) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }

  if (txn->GetState() != TransactionState::COMMITTED && txn->GetState() != TransactionState::ABORTED) {
    switch (txn->GetIsolationLevel()) {
      case IsolationLevel::REPEATABLE_READ:
        txn->SetState(TransactionState::SHRINKING);
        break;
      case IsolationLevel::READ_COMMITTED:
      case IsolationLevel::READ_UNCOMMITTED:
        if ((*pos)->lock_mode_ == LockMode::EXCLUSIVE) {
          txn->SetState(TransactionState::SHRINKING);
        }
        break;
    }
  }
  EraseFromTxn(txn, (*pos)->lock_mode_, oid, rid);
  delete (*pos);
  row_lock_map_ptr->request_queue_.erase(pos);
  row_lock_map_ptr->cv_.notify_all();
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

//void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {
//  std::unique_lock<std::mutex> lock(waits_for_latch_);
//  std::remove(waits_for_[t1].begin(), waits_for_[t1].end(), t2);
//}

auto LockManager::DFS(txn_id_t u, txn_id_t maxnid) -> txn_id_t {
  sett_.insert(u);
  book_.insert(u);
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
  for (const auto &x : waits_for_) {
    if (book_.find(x.first) == book_.end()) {
      sett_.clear();
      txn_id_t tmp = DFS(x.first, x.first);
      if (tmp != INVALID_TXN_ID) {
        *txn_id = tmp;
        return true;
      }
    }
  }
  return false;
}

void LockManager::CreateGraph() {
  for (const auto &x : table_lock_map_) {
    auto &que = (*x.second).request_queue_;
    for (auto u : que) {
      for (auto v : que) {
        if (u->txn_id_ != v->txn_id_ && !u->granted_ && v->granted_ &&
            !g_[static_cast<int>(u->lock_mode_)][static_cast<int>(v->lock_mode_)]) {
          AddEdge(u->txn_id_, v->txn_id_);
        }
      }
    }
  }
  for (const auto &x : row_lock_map_) {
    auto &que = (*x.second).request_queue_;
    for (auto u : que) {
      for (auto v : que) {
        if (u->txn_id_ != v->txn_id_ && !u->granted_ && v->granted_ &&
            !g_[static_cast<int>(u->lock_mode_)][static_cast<int>(v->lock_mode_)]) {
          AddEdge(u->txn_id_, v->txn_id_);
        }
      }
    }
  }
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
  if(enable_cycle_detection_) {
    txn->SetState(TransactionState::ABORTED);
  }
  std::unordered_set<LockRequestQueue *> informset;
  for (auto &x : table_lock_map_) {
    for (auto v : x.second->request_queue_) {
      if (v->txn_id_ == txn->GetTransactionId()) {
        informset.insert(x.second.get());
        break;
      }
    }
  }
  for (auto &x : row_lock_map_) {
    for (auto v : x.second->request_queue_) {
      if (v->txn_id_ == txn->GetTransactionId()) {
        informset.insert(x.second.get());
        break;
      }
    }
  }
  for (auto x : informset) {
    x->cv_.notify_all();
  }
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {  // TODO(students): detect deadlock
      txn_id_t txn_id;
      sett_.clear();
      book_.clear();
      CreateGraph();
      while (HasCycle(&txn_id)) {
        Transaction *txn = TransactionManager::GetTransaction(txn_id);
        if (enable_cycle_detection_) {
          Inform(txn);
        }
      }
    }
  }
}

auto LockManager::LockRequestQueue::GetLock(Transaction *txn, LockMode lock_mode, table_oid_t table_id,
                                            const bool g[][5]) -> bool {
  std::unique_lock<std::mutex> lock(latch_);
  auto grant = [&]() {
    // LOG_DEBUG("txnid: %d,status:%d",txn->GetTransactionId(),(int)txn->GetState());
    if (upgrading_ != INVALID_TXN_ID && upgrading_ != txn->GetTransactionId()) {
      return false;
    }
    for (auto req : request_queue_) {
      if (req->granted_ && !g[static_cast<int>(lock_mode)][static_cast<int>(req->lock_mode_)]) {
        return false;
      }
    }
    for (auto req : request_queue_) {
      if (req->txn_id_ == txn->GetTransactionId()) {
        req->granted_ = true;
        if (upgrading_ != INVALID_TXN_ID) {
          upgrading_ = INVALID_TXN_ID;
        }
        break;
      }
    }
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
        txn->GetSharedTableLockSet()->insert(table_id);
        break;
    }
    cv_.notify_all();
    return true;
  };
  while (!grant()) {
    cv_.wait(lock);
    if (txn->GetState() == TransactionState::ABORTED) {
      request_queue_.remove_if([&](LockRequest *&a) { return a->txn_id_ == txn->GetTransactionId(); });
      if (upgrading_ == txn->GetTransactionId()) {
        upgrading_ = INVALID_TXN_ID;
      }
      return false;
    }
  }
  return true;
}
auto LockManager::LockRequestQueue::GetLock(Transaction *txn, LockManager::LockMode lock_mode, table_oid_t table_id,
                                            RID rid, const bool g[][5]) -> bool {
  std::unique_lock<std::mutex> lock(latch_);
  auto grant = [&]() {
    if (upgrading_ != INVALID_TXN_ID && upgrading_ != txn->GetTransactionId()) {
      return false;
    }
    for (auto req : request_queue_) {
      if (req->granted_ && !g[static_cast<int>(lock_mode)][static_cast<int>(req->lock_mode_)]) {
        return false;
      }
    }
    for (auto req : request_queue_) {
      if (req->txn_id_ == txn->GetTransactionId()) {
        req->granted_ = true;
        if (upgrading_ != INVALID_TXN_ID) {
          upgrading_ = INVALID_TXN_ID;
        }
        break;
      }
    }
    switch (lock_mode) {
      case LockMode::SHARED:
        (*txn->GetSharedRowLockSet())[table_id].insert(rid);
        (*txn->GetSharedLockSet()).insert(rid);
        // LOG_DEBUG("txn:%d insertsrow",txn->GetTransactionId());
        // std::cout<<rid.ToString()<<std::endl;
        break;
      case LockMode::EXCLUSIVE:
        (*txn->GetExclusiveRowLockSet())[table_id].insert(rid);
        (*txn->GetExclusiveLockSet()).insert(rid);
        // LOG_DEBUG("txn:%d insertxrow",txn->GetTransactionId());
        // std::cout<<rid.ToString()<<std::endl;
        break;
      default:
        break;
    }
    cv_.notify_all();
    return true;
  };
  while (!grant()) {
    cv_.wait(lock);
    // LOG_DEBUG("txn:%d",txn->GetTransactionId());
    if (txn->GetState() == TransactionState::ABORTED) {
      request_queue_.remove_if([&](LockRequest *&a) { return a->txn_id_ == txn->GetTransactionId(); });
      if (upgrading_ == txn->GetTransactionId()) {
        upgrading_ = INVALID_TXN_ID;
      }
      return false;
    }
  }

  // LOG_DEBUG("txnid:%d",txn->GetTransactionId());
  return true;
}

}  // namespace bustub
