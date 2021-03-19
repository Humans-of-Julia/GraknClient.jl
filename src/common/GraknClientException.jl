# This file is a part of GraknClient.  License is MIT: https://github.com/Humans-of-Julia/GraknClient.jl/blob/main/LICENSE 

# 
# package grakn.client.common;
# 
# import io.grpc.Status;
# import io.grpc.StatusRuntimeException;
# 
# import javax.annotation.Nullable;
# 
# public class GraknClientException extends RuntimeException {
# 
#     @Nullable
#     private final ErrorMessage errorMessage;
# 
#     public GraknClientException(ErrorMessage error, Object... parameters) {
#         super(error.message(parameters));
#         assert !getMessage().contains("%s");
#         this.errorMessage = error;
#     }
# 
#     public GraknClientException(String message, Throwable cause) {
#         super(message, cause);
#         this.errorMessage = null;
#     }
# 
#     public static GraknClientException of(StatusRuntimeException statusRuntimeException) {
#         // "Received Rst Stream" occurs if the server is in the process of shutting down.
#         if (statusRuntimeException.getStatus().getCode() == Status.Code.UNAVAILABLE
#                 || statusRuntimeException.getStatus().getCode() == Status.Code.UNKNOWN
#                 || statusRuntimeException.getMessage().contains("Received Rst Stream")) {
#             return new GraknClientException(ErrorMessage.Client.UNABLE_TO_CONNECT);
#         } else if (isReplicaNotPrimaryException(statusRuntimeException)) {
#             return new GraknClientException(ErrorMessage.Client.CLUSTER_REPLICA_NOT_PRIMARY);
#         }
#         return new GraknClientException(statusRuntimeException.getStatus().getDescription(), statusRuntimeException);
#     }
# 
#     public String getName() {
#         return this.getClass().getName();
#     }
# 
#     @Nullable
#     public ErrorMessage getErrorMessage() {
#         return errorMessage;
#     }
# 
#     // TODO: propagate exception from the server side in a less-brittle way
#     private static boolean isReplicaNotPrimaryException(StatusRuntimeException statusRuntimeException) {
#         return statusRuntimeException.getStatus().getCode() == Status.Code.INTERNAL && statusRuntimeException.getStatus().getDescription().contains("[RPL01]");
#     }
# }
