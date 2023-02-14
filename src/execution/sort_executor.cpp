#include "execution/executors/sort_executor.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void SortExecutor::Init() {
  child_executor_->Init();
  tuple_array_.clear();
  Tuple child_tuple;
  RID child_rid;
  while (child_executor_->Next(&child_tuple, &child_rid)) {
    tuple_array_.emplace_back(child_tuple);
  }
  std::sort(tuple_array_.begin(), tuple_array_.end(), [&](const Tuple &a, const Tuple &b) {
    for (auto &x : plan_->GetOrderBy()) {
      auto a_value = x.second->Evaluate(&a, GetOutputSchema());
      auto b_value = x.second->Evaluate(&b, GetOutputSchema());
      if (a_value.CompareEquals(b_value) == CmpBool::CmpTrue) {
        continue;
      }
      if (x.first == OrderByType::DEFAULT || x.first == OrderByType::ASC) {
        return a_value.CompareLessThan(b_value);
      }
      return a_value.CompareGreaterThan(b_value);
    }
    return CmpBool::CmpTrue;
  });
  iter_ = tuple_array_.cbegin();
  // throw NotImplementedException("SortExecutor is not implemented");
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (iter_ == tuple_array_.cend()) {
    return false;
  }
  *tuple = *iter_;
  *rid = tuple->GetRid();
  iter_++;
  return true;
}

}  // namespace bustub
