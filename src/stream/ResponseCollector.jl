# This file is a part of GraknClient.  License is MIT: https://github.com/Humans-of-Julia/GraknClient.jl/blob/main/LICENSE

mutable struct ResponseCollector
    collectors::Union{Nothing,Dict{String,Channel}}
    transact_result_channel::Channel
end

function ResponseCollector()
    dict = Dict{String,Channel}()
    return ResponseCollector(dict,Channel())
end

function ResponseCollector(transact_result_channel::Channel)
    resp_col = ResponseCollector()
    resp_col.transact_result_channel = transact_result_channel
    return resp_col
end

function response_worker(response_collector::ResponseCollector)
    resp_chan = response_collector.transact_result_channel
    while isOpen(resp_chan)
        if isready(resp_chan)
            result_srv = take!(resp_chan)
            which_result = which_oneof(result_srv, :server)
            tmp_result = getproperty(result_srv, which_result)
            id = tmp_result.req_id
            
        end
    end
end
#
# package grakn.client.stream;
#
# import grakn.client.common.exception.GraknClientException;
# import grakn.common.collection.Either;
# import io.grpc.StatusRuntimeException;
#
# import javax.annotation.Nullable;
# import java.util.Optional;
# import java.util.UUID;
# import java.util.concurrent.BlockingQueue;
# import java.util.concurrent.ConcurrentHashMap;
# import java.util.concurrent.ConcurrentMap;
# import java.util.concurrent.LinkedBlockingQueue;
#
# import static grakn.client.common.exception.ErrorMessage.Client.TRANSACTION_CLOSED;
# import static grakn.client.common.exception.ErrorMessage.Internal.UNEXPECTED_INTERRUPTION;
#
# public class ResponseCollector<R> {
#
#     private final ConcurrentMap<UUID, Queue<R>> collectors;
#
#     public ResponseCollector() {
#         collectors = new ConcurrentHashMap<>();
#     }
#
#     public synchronized Queue<R> queue(UUID requestId) {
#         Queue<R> collector = new Queue<>();
#         collectors.put(requestId, collector);
#         return collector;
#     }
#
#     public Queue<R> get(UUID requestId) {
#         return collectors.get(requestId);
#     }
#
#     public synchronized void close(@Nullable StatusRuntimeException error) {
#         collectors.values().forEach(collector -> collector.close(error));
#         collectors.clear();
#     }
#
#     public static class Queue<R> {
#
#         private final BlockingQueue<Either<Response<R>, Done>> responseQueue;
#
#         Queue() {
#             responseQueue = new LinkedBlockingQueue<>();
#         }
#
#         public R take() {
#             try {
#                 Either<Response<R>, Done> response = responseQueue.take();
#                 if (response.isFirst()) return response.first().message();
#                 else if (!response.second().error().isPresent()) throw new GraknClientException(TRANSACTION_CLOSED);
#                 else throw GraknClientException.of(response.second().error().get());
#             } catch (InterruptedException e) {
#                 throw new GraknClientException(UNEXPECTED_INTERRUPTION);
#             }
#         }
#
#         public void put(R response) {
#             responseQueue.add(Either.first(new Response<>(response)));
#         }
#
#         public void close(@Nullable StatusRuntimeException error) {
#             responseQueue.add(Either.second(new Done(error)));
#         }
#
#         private static class Response<R> {
#
#             @Nullable
#             private final R value;
#
#             private Response(@Nullable R value) {
#                 this.value = value;
#             }
#
#             @Nullable
#             private R message() {
#                 return value;
#             }
#         }
#
#         private static class Done {
#             @Nullable
#             private final StatusRuntimeException error;
#
#             private Done(StatusRuntimeException error) {
#                 this.error = error;
#             }
#
#             private Optional<StatusRuntimeException> error() {
#                 return Optional.ofNullable(error);
#             }
#         }
#     }
# }
