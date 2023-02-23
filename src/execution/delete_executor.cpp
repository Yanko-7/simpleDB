//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
  child_executor_->Init();
  try {
    if (!exec_ctx_->GetLockManager()->LockTable(exec_ctx_->GetTransaction(), LockManager::LockMode::INTENTION_EXCLUSIVE,
                                                plan_->TableOid())) {
      throw ExecutionException("1delete lock IN_table fail");
    }
  } catch (TransactionAbortException &e) {
    throw ExecutionException("2delete lock IN_table fail");
  }
  has_output_ = false;
  // throw NotImplementedException("DeleteExecutor is not implemented");
}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (has_output_) {
    return false;
  }
  std::vector<Value> values;
  std::vector<Column> columns;
  columns.emplace_back("delete_row_count", INTEGER);
  Schema schema(columns);
  //
  auto table_info = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  Tuple child_tuple;
  RID child_rid;
  int cnt = 0;
  while (child_executor_->Next(&child_tuple, &child_rid)) {
    try {
      if (!exec_ctx_->GetLockManager()->LockRow(exec_ctx_->GetTransaction(), LockManager::LockMode::EXCLUSIVE,
                                                plan_->TableOid(), child_rid)) {
        throw ExecutionException("delete lock row fail");
      }
    } catch (TransactionAbortException &e) {
      throw ExecutionException("delete lock IN_row fail");
    }
    if (table_info->table_->MarkDelete(child_rid, exec_ctx_->GetTransaction())) {
      cnt++;
      for (auto &index_info : exec_ctx_->GetCatalog()->GetTableIndexes(table_info->name_)) {
        auto key =
            child_tuple.KeyFromTuple(table_info->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
        index_info->index_->DeleteEntry(key, child_rid, exec_ctx_->GetTransaction());
      }
    }
  }
  // update index
  values.emplace_back(INTEGER, cnt);
  *tuple = Tuple(values, &schema);
  has_output_ = true;
  return true;
}

}  // namespace bustub
