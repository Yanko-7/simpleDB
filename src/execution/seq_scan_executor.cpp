//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void SeqScanExecutor::Init() {
  if (exec_ctx_->GetTransaction()->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
    try {
      if (!exec_ctx_->GetLockManager()->LockTable(exec_ctx_->GetTransaction(), LockManager::LockMode::INTENTION_SHARED,
                                                  plan_->GetTableOid())) {
        throw ExecutionException("1Seq lock table fail");
      }
    } catch (TransactionAbortException &e) {
      throw ExecutionException(e.GetInfo());
    }
  }
  it_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())->table_->Begin(exec_ctx_->GetTransaction());
  // throw NotImplementedException("SeqScanExecutor is not implemented");
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (it_ == exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())->table_->End()) {
    return false;
  }
  bool flag = false;
  *tuple = *it_;
  if (exec_ctx_->GetTransaction()->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
    try {
      if (!exec_ctx_->GetLockManager()->LockRow(exec_ctx_->GetTransaction(), LockManager::LockMode::SHARED,
                                                plan_->GetTableOid(), tuple->GetRid())) {
        throw ExecutionException("1Seq lock row fail");
      }
    } catch (TransactionAbortException &e) {
      throw ExecutionException(e.GetInfo());
    }
    flag = true;
  }
  *rid = tuple->GetRid();
  if (flag && exec_ctx_->GetTransaction()->GetIsolationLevel() != IsolationLevel::REPEATABLE_READ) {
    try {
      if (!exec_ctx_->GetLockManager()->UnlockRow(exec_ctx_->GetTransaction(), plan_->GetTableOid(), it_->GetRid())) {
        throw ExecutionException("1Seq unlock row fail");
      }
    } catch (TransactionAbortException &e) {
      throw ExecutionException("2Seq unlock row fail");
    }
  }
  it_++;
  return true;
}

}  // namespace bustub
