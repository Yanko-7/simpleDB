#include "execution/executors/topn_executor.h"

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void TopNExecutor::Init() {
  child_executor_->Init();
  result_.clear();
  Tuple child_tuple;
  RID child_rid;
  auto comparator = [&](const Tuple &a, const Tuple &b) {
    for (auto &x : plan_->GetOrderBy()) {
      auto a_value = x.second->Evaluate(&a, GetOutputSchema());
      auto b_value = x.second->Evaluate(&b, GetOutputSchema());
      if (a_value.CompareEquals(b_value) == CmpBool::CmpTrue) {
        continue;
      }
      if (x.first == OrderByType::DEFAULT || x.first == OrderByType::ASC) {
        return static_cast<bool>(a_value.CompareLessThan(b_value));
      }
      return static_cast<bool>(a_value.CompareGreaterThan(b_value));
    }
    return true;
  };
  std::priority_queue<Tuple, std::vector<Tuple>, decltype(comparator)> que(comparator);
  while (child_executor_->Next(&child_tuple, &child_rid)) {
    if (que.size() == plan_->GetN()) {
      if (comparator(child_tuple, que.top())) {
        que.pop();
        que.push(child_tuple);
      }
      continue;
    }
    que.push(child_tuple);
  }
  while (!que.empty()) {
    result_.push_back(que.top());
    que.pop();
  }
  // throw NotImplementedException("TopNExecutor is not implemented");
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (result_.empty()) {
    return false;
  }
  *tuple = result_.back();
  result_.pop_back();
  *rid = tuple->GetRid();
  return true;
}

}  // namespace bustub
