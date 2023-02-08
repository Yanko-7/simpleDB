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
    : AbstractExecutor(exec_ctx),plan_(plan),child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
  child_executor_->Init();
  has_output = false;
  //throw NotImplementedException("DeleteExecutor is not implemented");
}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if(has_output){ return false; }
  std::vector<Value>values;
  std::vector<Column>columns;
  columns.emplace_back("delete_row_count",INTEGER);
  Schema schema(columns);
  //
  auto table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  Tuple tuple_;
  RID rid_ ;
  int cnt = 0;
  while (child_executor_->Next(&tuple_,&rid_)){
    if(table_info_->table_->MarkDelete(rid_,exec_ctx_->GetTransaction())){
      cnt++;
      for(auto &index_info : exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_)){
        auto key = tuple_.KeyFromTuple(table_info_->schema_,index_info->key_schema_,index_info->index_->GetKeyAttrs());
        index_info->index_->DeleteEntry(key,rid_,exec_ctx_->GetTransaction());
      }
    };
  }
  //update index
  values.emplace_back(INTEGER,cnt);
  *tuple = Tuple(values,&schema);
  has_output = true;
  return true;
}

}  // namespace bustub
