# This file is a part of GraknClient.  License is MIT: https://github.com/Humans-of-Julia/GraknClient.jl/blob/main/LICENSE

struct Relation <: AbstractThing
    iid::String
    type::RelationType
end

function Relation(t::Proto.Thing)
    iid = bytes2hex(t.iid)
    isempty(iid) && throw(GraknClientException(CONCEPT_MISSING_IID))
    return Relation(iid, instantiate(t._type))
end

# TODO seems unnecessary?
as_relation(r::Relation) = r

# Remote functions

# TODO depends on concepts: RoleType, Thing
