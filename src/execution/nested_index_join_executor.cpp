//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_index_join_executor.cpp
//
// Identification: src/execution/nested_index_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_index_join_executor.h"

namespace bustub {

NestIndexJoinExecutor::NestIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan),child_executor_(std::move(child_executor)){
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestIndexJoinExecutor::Init() {
  child_executor_->Init();
  //throw NotImplementedException("NestIndexJoinExecutor is not implemented");
}

auto NestIndexJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  Tuple child_tuple;
  RID child_rid;
  while (true) {
    if (!child_executor_->Next(&child_tuple, &child_rid)) {
      return false;
    }
    auto index_tree = dynamic_cast<BPlusTreeIndexForOneIntegerColumn *>(
        exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid())->index_.get());
    auto &expr = plan_->KeyPredicate();
    auto value = expr->Evaluate(&child_tuple, child_executor_->GetOutputSchema());
    std::vector<Value> tmp_values;
    tmp_values.push_back(value);
    std::vector<Column> tmp_columns;
    tmp_columns.emplace_back("", value.GetTypeId());
    Schema tmp_schema(tmp_columns);
    std::vector<RID> result;
    index_tree->ScanKey(Tuple(tmp_values, &tmp_schema), &result, exec_ctx_->GetTransaction());
    if (result.empty()) {
      if (plan_->GetJoinType() == JoinType::LEFT) {
        auto schmea = GetOutputSchema();
        std::vector<Value> values;
        values.reserve(schmea.GetColumnCount());
        for (uint32_t i = 0; i < child_executor_->GetOutputSchema().GetColumnCount(); i++) {
          values.emplace_back(child_tuple.GetValue(&child_executor_->GetOutputSchema(), i));
        }
        for (uint32_t i = 0; i < plan_->InnerTableSchema().GetColumnCount(); i++) {
          values.emplace_back(ValueFactory::GetNullValueByType(plan_->InnerTableSchema().GetColumn(i).GetType()));
        }
        *tuple = Tuple(values, &schmea);
        return true;
      }
      continue;
    }
    auto tmp_rid = result[0];
    Tuple right_tuple;
    exec_ctx_->GetCatalog()
        ->GetTable(plan_->GetInnerTableOid())
        ->table_->GetTuple(tmp_rid, &right_tuple, exec_ctx_->GetTransaction());

    auto schmea = GetOutputSchema();
    std::vector<Value> values;
    values.reserve(schmea.GetColumnCount());
    for (uint32_t i = 0; i < child_executor_->GetOutputSchema().GetColumnCount(); i++) {
      values.emplace_back(child_tuple.GetValue(&child_executor_->GetOutputSchema(), i));
    }
    for (uint32_t i = 0; i < plan_->InnerTableSchema().GetColumnCount(); i++) {
      values.emplace_back(right_tuple.GetValue(&plan_->InnerTableSchema(), i));
    }
    *tuple = Tuple(values, &schmea);
    return true;
  }
}

}  // namespace bustub
