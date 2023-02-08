//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan){}

void IndexScanExecutor::Init() {
  tree_ = dynamic_cast<BPlusTreeIndexForOneIntegerColumn *>(exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid())->index_.get());
  iter_ = tree_->GetBeginIterator();
  //throw NotImplementedException("IndexScanExecutor is not implemented");
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if(iter_ == tree_->GetEndIterator()){ return false; }
  *rid = (*iter_).second;
  auto page = static_cast<TablePage *>(exec_ctx_->GetBufferPoolManager()->FetchPage(rid->GetPageId()));
  if(!page->GetTuple(*rid,tuple,exec_ctx_->GetTransaction(),exec_ctx_->GetLockManager())){
  }
  ++iter_;
  return true;
}

}  // namespace bustub
