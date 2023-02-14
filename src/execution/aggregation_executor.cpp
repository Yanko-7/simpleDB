//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {
AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_(std::move(child)),
      aht_(plan->GetAggregates(), plan->GetAggregateTypes()),
      aht_iterator_(aht_.Begin()) {}

void AggregationExecutor::Init() {
  has_out_ = false;
  aht_.Clear();
  child_->Init();
  Tuple child_tuple;
  RID rid;
  while (true) {
    if (!child_->Next(&child_tuple, &rid)) {
      break;
    }
    aht_.InsertCombine(MakeAggregateKey(&child_tuple), MakeAggregateValue(&child_tuple));
  }
  aht_iterator_ = aht_.Begin();
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (aht_iterator_ == aht_.End()) {
    if (!has_out_) {
      auto tmp = aht_.GenerateInitialAggregateValue().aggregates_;
      if (tmp.size() != plan_->OutputSchema().GetColumnCount()) {
        return false;
      }
      *tuple = Tuple(tmp, &plan_->OutputSchema());
      *rid = tuple->GetRid();
      has_out_ = true;
      return true;
    }
    return false;
  }
  has_out_ = true;
  std::vector<Value> values = aht_iterator_.Key().group_bys_;
  values.insert(values.end(), aht_iterator_.Val().aggregates_.begin(), aht_iterator_.Val().aggregates_.end());
  *tuple = Tuple(values, &plan_->OutputSchema());
  *rid = tuple->GetRid();
  ++aht_iterator_;
  return true;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_.get(); }

}  // namespace bustub
