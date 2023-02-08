//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),plan_(plan),left_executor_(std::move(left_executor)),right_executor_(std::move(right_executor)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedLoopJoinExecutor::Init() {
  is_empty = false;
  has_out = false;
  left_executor_->Init();
  right_executor_->Init();
  is_empty = !left_executor_->Next(&left_tuple_,&rid_);
  //throw NotImplementedException("NestedLoopJoinExecutor is not implemented");
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if(is_empty){ return false; }
  while (true){
    if(!right_executor_->Next(&right_tuple_,&rid_)){
      if(!has_out && plan_->GetJoinType() == JoinType::LEFT){
        auto tmp_schmea = bustub::NestedLoopJoinPlanNode::InferJoinSchema(*plan_->GetLeftPlan(),*plan_->GetRightPlan());
        std::vector<Value>values;
        values.reserve(tmp_schmea.GetColumnCount());
        for(uint32_t i = 0;i < left_executor_->GetOutputSchema().GetColumnCount(); i ++){
          values.emplace_back(left_tuple_.GetValue(&left_executor_->GetOutputSchema(),i));
        }
        for(uint32_t i = 0;i < right_executor_->GetOutputSchema().GetColumnCount(); i ++){
          values.emplace_back(ValueFactory::GetNullValueByType(right_executor_->GetOutputSchema().GetColumn(i).GetType()));
        }
        *tuple = Tuple(values,&tmp_schmea);
        has_out = true;
        return true;
      }
      has_out = false;
      if(!left_executor_->Next(&left_tuple_,&rid_)){
        return false;
      }
      right_executor_->Init();
      continue ;
    }
    auto value = plan_->Predicate().EvaluateJoin(&left_tuple_,left_executor_->GetOutputSchema(),&right_tuple_,right_executor_->GetOutputSchema());
    if(!value.IsNull() &&value.GetAs<bool>()){
      auto tmp_schmea = bustub::NestedLoopJoinPlanNode::InferJoinSchema(*plan_->GetLeftPlan(),*plan_->GetRightPlan());
      std::vector<Value>values;
      values.reserve(tmp_schmea.GetColumnCount());
      for(uint32_t i = 0;i < left_executor_->GetOutputSchema().GetColumnCount(); i ++){
        values.emplace_back(left_tuple_.GetValue(&left_executor_->GetOutputSchema(),i));
      }
      for(uint32_t i = 0;i < right_executor_->GetOutputSchema().GetColumnCount(); i ++){
        values.emplace_back(right_tuple_.GetValue(&right_executor_->GetOutputSchema(),i));
      }
      *tuple = Tuple(values,&tmp_schmea);
      has_out = true;
      return true;
    }
  }
}

}  // namespace bustub
