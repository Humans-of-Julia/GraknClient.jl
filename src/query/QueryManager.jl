# This file is a part of TypeDBClient.  License is MIT: https://github.com/Humans-of-Julia/TypeDBClient.jl/blob/main/LICENSE

function match(transaction::AbstractCoreTransaction, query::String, options = Proto.Options())
    db_result =  execute(transaction, QueryManagerRequestBuilder.match_req(query, options))
    if db_result !== nothing
        maps = map(x->ConceptMap(x), [entry.query_manager_res_part.match_res_part.answers for entry in db_result])
        result = reduce(vcat, maps)
    else
        result = nothing
    end
    return result
end

function match_aggregate(transaction::AbstractCoreTransaction, query::String, options = Proto.Options())
    db_result =  execute(transaction, QueryManagerRequestBuilder.match_aggregate_req(query, options))
    if db_result !== nothing
        result = [_read_proto_number(entry.query_manager_res.match_aggregate_res.answer) for entry in db_result]
    else
        result = nothing
    end
    return result
end

function match_group(transaction::AbstractCoreTransaction, query::String, options = Proto.Options())
    db_result =  execute(transaction, QueryManagerRequestBuilder.match_group_req(query, options))
    if db_result !== nothing
        maps =  [ConceptMapGroup(item.query_manager_res_part.match_group_res_part.answers) for item in db_result]
        result = reduce(vcat, maps)
    else
        result = nothing
    end
    return result
end

function match_group_aggregate(transaction::AbstractCoreTransaction, query::String, options = Proto.Options())
    db_result =  execute(transaction, QueryManagerRequestBuilder.match_group_aggregate_req(query, options))
    if db_result !== nothing
        maps = [NumericGroup(item.match_group_aggregate_res_part.answers) for item in db_result]
        result = reduce(vcat, maps)
    else
        result = nothing
    end
    return result
end

function insert(transaction::AbstractCoreTransaction, query::String, options = Proto.Options())
    db_result = execute(transaction, QueryManagerRequestBuilder.insert_req(query, options))
    if db_result !== nothing
        answers = [entry.query_manager_res_part.insert_res_part.answers for entry in db_result]
        maps = map(ConceptMap, answers)
        result = reduce(vcat, maps)
    else
        result = nothing
    end
    return result
end

function delete(transaction::AbstractCoreTransaction, query::String, options = Proto.Options())
    execute(transaction, QueryManagerRequestBuilder.delete_req(query, options))
    return nothing
end

function update(transaction::AbstractCoreTransaction, query::String, options = Proto.Options())
    db_result = execute(transaction, QueryManagerRequestBuilder.update_req(query, options))
    if db_result !== nothing
        answers = [entry.query_manager_res_part.update_res_part.answers for entry in db_result]
        maps = map(ConceptMap, answers)
        result = reduce(vcat, maps)
    else
        result = nothing
    end
    return result
end

function explain(transaction::AbstractCoreTransaction, explainable::AbstractExplainable, options = Proto.Options())
    @error "explain isn't implemented yet"
    # return (_Explanation.of(ex) for rp in self.stream(query_manager_explain_req(explainable.explainable_id(), options.proto())) for ex in rp.explain_res_part.explanations)
end

function define(transaction::AbstractCoreTransaction, query::String, options = Proto.Options())
    db_result = execute(transaction, QueryManagerRequestBuilder.define_req(query, options))
    return nothing
end

function undefine(transaction::AbstractCoreTransaction, query::String, options = Proto.Options())
    db_result = execute(transaction, QueryManagerRequestBuilder.undefine_req(query, options))
    return nothing
end
