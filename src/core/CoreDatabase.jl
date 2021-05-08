# This file is a part of TypeDBClient.  License is MIT: https://github.com/Humans-of-Julia/TypeDBClient.jl/blob/main/LICENSE

mutable struct CoreDatabase <: AbstractCoreDatabaseManager
    name::String
end

function schema_database(db::T, core_client::R) where {T<:AbstractCoreDatabaseManager, R<:AbstractCoreClient}
    return database_schema(core_client.core_stub, gRPCController(), DatabaseRequestBuilder.schema_req(db.name)).schema
end
