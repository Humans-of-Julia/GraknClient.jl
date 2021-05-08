function grpc_result_or_error(result::T,
    status::Task,
    f::Function) where {T<:Union{<:Proto.ProtoType,<:Channel{<:Proto.ProtoType},<:Nothing}}

    istaskdone(status) && !(fetch(status).success) &&
        throw(TypeDBClientException(fetch(status).message, gRPCServiceCallException(fetch(status).grpc_status,fetch(status).message)))
    result === nothing &&
        throw(TypeDBClientException("something went wrong in gRPC", gRPCServiceCallException(0, "Error not defined by gRPC")))
    f(result)

end
