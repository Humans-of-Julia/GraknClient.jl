# This file is a part of TypeDBClient.  License is MIT: https://github.com/Humans-of-Julia/TypeDBClient.jl/blob/main/LICENSE

struct ConceptManager{T <: AbstractCoreTransaction} <: AbstractConceptManager
    txn::T
end

struct Root
end

# Get root something type. The hard coded strings came from GraqlToken
Base.get(cm::ConceptManager, ::Type{EntityType}, ::Root) = get(cm, ThingType, "thing")
Base.get(cm::ConceptManager, ::Type{RelationType}, ::Root) = get(cm, ThingType, "relation")
Base.get(cm::ConceptManager, ::Type{AttributeType}, ::Root) = get(cm, ThingType, "attribute")
Base.get(cm::ConceptManager, ::Type{ThingType}, ::Root) = get(cm, ThingType, "thing")

function Base.get(cm::ConceptManager, ::Type{EntityType}, label::String)
    t = get(cm, ThingType, label)
    t !== nothing && t isa EntityType && return t
    return nothing
end

function put(cm::ConceptManager, ::Type{EntityType}, label::String)
    res = execute(cm, ConceptManagerRequestBuilder.put_entity_type_req(label))
    if which_oneof(res, :res) == :put_entity_type_res
        return instantiate(res.put_entity_type_res.entity_type)
    end
    return nothing
end

function Base.get(cm::ConceptManager, ::Type{RelationType}, label::String)
    t = get(cm, ThingType, label)
    t !== nothing && t isa RelationType && return t
    return nothing
end

function put(cm::ConceptManager, ::Type{RelationType}, label::String)
    res = execute(cm, ConceptManagerRequestBuilder.put_relation_type_req(label))
    if which_oneof(res, :res) == :put_relation_type_res
        return instantiate(res.put_relation_type_res.relation_type)
    end
    return nothing
end

function Base.get(cm::ConceptManager, ::Type{AttributeType}, label::String)
    t = get(cm, ThingType, label)
    t !== nothing && t isa AttributeType && return t
    return nothing
end

function put(cm::ConceptManager, ::Type{AttributeType}, label::String, value_type::EnumType)
    res = execute(cm, ConceptManagerRequestBuilder.put_attribute_type_req(label, value_type))
    if which_oneof(res, :res) == :put_attribute_type_res
        return instantiate(res.put_attribute_type_res.attribute_type)
    end
    return nothing
end

function Base.get(cm::ConceptManager, ::Type{ThingType}, label::String)
    req = ConceptManagerRequestBuilder.get_thing_type_req(label)
    res = execute(cm, req)
    if which_oneof(res, :res) == :get_thing_type_res
        return instantiate(res.get_thing_type_res.thing_type)
    end
    return nothing
end

function Base.get(cm::ConceptManager, ::Type{Thing}, iid::String)
    req = ConceptManagerRequestBuilder.get_thing_req(iid)
    res = execute(cm, req)
    if which_oneof(res, :res) == :get_thing_res
        return instantiate(res.get_thing_res.thing)
    end
    return nothing
end

# TODO execute request and returns Proto.ConceptManager_Res
function execute(cm::ConceptManager, req::Proto.Transaction_Req)
    @error "Not implemented yet."
end
