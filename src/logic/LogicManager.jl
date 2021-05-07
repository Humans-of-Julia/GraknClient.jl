# This file is a part of TypeDBClient.  License is MIT: https://github.com/Humans-of-Julia/TypeDBClient.jl/blob/main/LICENSE

mutable struct LogicManager <: AbstractLogicManager
    transaction::AbstractCoreTransaction
end

function get_rule(log_mgr::AbstractLogicManager, label::String)
    res = execute(log_mgr.transaction, LogicManagerRequestBuilder.get_rule_req(label))
    result = if which_oneof(res.logic_manager_res.get_rule_res, :res) == :rule
                Rule(res.logic_manager_res.get_rule_res.rule)
             else
                nothing
             end
    return result
end

function get_rules(log_mgr::AbstractLogicManager)
    dbResult = stream(log_mgr.transaction, LogicManagerRequestBuilder.get_rules_req())
    answers = [res.logic_manager_res_part.get_rules_res_part.rules for res in dbResult]
    rules = map(Rule,answers)
    result = reduce(vcat, rules)
    return result
end

function put_rule(log_mgr::AbstractLogicManager, label::String, when::String, then::String)
    res = execute(log_mgr.transaction, LogicManagerRequestBuilder.put_rule_req(label, when, then))
    result = Rule(res.logic_manager_res.get_rule_res.rule)
    return result
end
