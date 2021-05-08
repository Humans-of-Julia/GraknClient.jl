# This file is a part of TypeDBClient.  License is MIT: https://github.com/Humans-of-Julia/TypeDBClient.jl/blob/main/LICENSE

struct ResponseCollector
    collectors::Dict{Bytes,Channel{Transaction_Res_All}}
    transact_result_channel::Channel{Proto.Transaction_Server}
    access_lock::ReentrantLock
end


function ResponseCollector(transact_result_channel::Channel{Proto.Transaction_Server})
    collectors = Dict{Bytes,Channel{Transaction_Res_All}}()
    access_lock = ReentrantLock()
    resp_col = ResponseCollector(collectors, transact_result_channel, access_lock)
    res_task = @async response_worker(resp_col)
    return resp_col
end

"""
    push!(resp_collector::ResponsCollector, request::T) where {T<:Proto.ProtoType}
Function is meant to give back the result_channel in which the results for one request
will be collected.
Attention! Don't put a new Id manually on the ResponsCollector. It wouldn't be thread safe
"""
function Base.push!(resp_collector::ResponseCollector, req_id::Bytes)
    res_channel = Channel{Transaction_Res_All}(10)
    try
        lock(resp_collector.access_lock)
        resp_collector.collectors[req_id] = res_channel
    finally
        unlock(resp_collector.access_lock)
    end
    return res_channel
end

"""
    delete!(resp_collector::ResponsCollector, id::Bytes)
The function will close the collecting result channel and remove
this from the the resonse collector.
Attention! Don't remove a result_channel manually from the Dictionary.
This will not be thread safe.
"""
function Base.delete!(resp_collector::ResponseCollector, id::Bytes)
    try
        lock(resp_collector.access_lock)
        close(resp_collector.collectors[id])
        delete!(resp_collector.collectors, id)
    finally
        unlock(resp_collector.access_lock)
    end
end

function response_worker(response_collector::ResponseCollector)
    resp_chan = response_collector.transact_result_channel
    collectors = response_collector.collectors
    while isopen(resp_chan)
        yield()
        try
            if isready(resp_chan)
                req_result = take!(resp_chan)
                tmp_result = _process_Transaction_Server(req_result)
                put!(collectors[tmp_result.req_id], tmp_result)
            end
        catch ex
            @info "response_worker shows an error \n
            $ex"
        end
    end
    @info "response_collector is Done"
end

function _process_Transaction_Server(input::Proto.Transaction_Server)
    kind_of_respond = Proto.which_oneof(input,:server)
    result = getproperty(input, kind_of_respond)
    return result
end

function close(res_collector::ResponseCollector)
    #close the resul channels which are open
    close.(values(res_collector.collectors))
end
