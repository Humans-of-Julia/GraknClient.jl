########## precompiling section

@assert precompile(meta, (Type{Proto.Type_Res}, ))
@assert precompile(meta, (Type{Proto.Type_Req}, ))
@assert precompile(meta, (Type{Proto.Transaction_Res}, ))
@assert precompile(meta, (Type{Proto.Transaction_ResPart}, ))

@assert precompile(which_oneof, (Proto.Transaction_ResPart, Symbol))
@assert precompile(which_oneof, (Proto.Transaction_Res, Symbol))
@assert precompile(_process_request, (BidirectionalStream, Proto.Transaction_Req, Bool))
@assert precompile(_open_result_channel, (BidirectionalStream, Proto.Transaction_Req, Bool))
@assert precompile(collect_result, (Channel{Union{Proto.Transaction_Res,Proto.Transaction_ResPart}}, BidirectionalStream))
@assert precompile(_is_stream_respart_done, (Proto.Transaction_Res, BidirectionalStream))
@assert precompile(_is_stream_respart_done, (Proto.Transaction_ResPart, BidirectionalStream))
