# This file is a part of TypeDBClient.  License is MIT: https://github.com/Humans-of-Julia/TypeDBClient.jl/blob/main/LICENSE
abstract type AbstractTypeDBStub end

mutable struct Core_TypeDBStub <: AbstractTypeDBStub
    blockingStub::GraknCoreBlockingStub
    asyncStub::GraknCoreStub
end

function Core_TypeDBStub(channel::gRPCClient.gRPCChannel)
    blockingStub = GraknCoreBlockingStub(channel)
    asyncStub = GraknCoreStub(channel)
    return Core_TypeDBStub(blockingStub,asyncStub)
end

mutable struct Cluster_TypeDBStub <: AbstractTypeDBStub
    blockingStub::GraknCoreBlockingStub
end

function Cluster_TypeDBStub(channel::gRPCClient.gRPCChannel)
    blockingStub = GraknCoreBlockingStub(channel)
    return Cluster_TypeDBStub(blockingStub)
end

function ensure_connected(stub::T) where {T<:AbstractTypeDBStub}
    throw(TypeDBClientException(GENERAL_UNKOWN_ERROR,"function TypeDBStub.jl/ensure_onnected isn't implemented yet"))
end
#
# package typedb.client.common.rpc;
#
# import typedb.client.common.exception.TypeDBClientException;
# import typedb.protocol.ClusterServerProto;
# import typedb.protocol.CoreDatabaseProto.CoreDatabase;
# import typedb.protocol.CoreDatabaseProto.CoreDatabaseManager;
# import typedb.protocol.TypeDBClusterGrpc;
# import typedb.protocol.TypeDBGrpc;
# import typedb.protocol.SessionProto.Session;
# import typedb.protocol.TransactionProto;
# import io.grpc.ConnectivityState;
# import io.grpc.ManagedChannel;
# import io.grpc.StatusRuntimeException;
# import io.grpc.stub.StreamObserver;
#
# import java.util.function.Supplier;
#
# import static typedb.protocol.ClusterDatabaseProto.ClusterDatabaseManager;
#
# public abstract class TypeDBStub {
#
#     private final ManagedChannel channel;
#
#     private TypeDBStub(ManagedChannel channel) {
#         this.channel = channel;
#     }
#
#     public static Core core(ManagedChannel channel) {
#         return new Core(channel);
#     }
#
#     public static Cluster cluster(ManagedChannel channel) {
#         return new Cluster(channel);
#     }
#
#     private void ensureConnected() {
#         // The Channel is a persistent HTTP connection. If it gets interrupted (say, by the server going down) then
#         // gRPC's recovery logic will kick in, marking the Channel as being in a transient failure state and rejecting
#         // all RPC calls while in this state. It will attempt to reconnect periodically in the background, using an
#         // exponential backoff algorithm. Here, we ensure that when the user needs that connection urgently (e.g: to
#         // open a TypeDB session), it tries to reconnect immediately instead of just failing without trying.
#         if (channel.getState(true).equals(ConnectivityState.TRANSIENT_FAILURE)) {
#             channel.resetConnectBackoff();
#         }
#     }
#
#     protected <RES> RES resilientCall(Supplier<RES> function) {
#         try {
#             ensureConnected();
#             return function.get();
#         } catch (StatusRuntimeException e) {
#             throw TypeDBClientException.of(e);
#         }
#     }
#
#     public static class Core extends TypeDBStub {
#
#         private final TypeDBGrpc.TypeDBBlockingStub blockingStub;
#         private final TypeDBGrpc.TypeDBStub asyncStub;
#
#         private Core(ManagedChannel channel) {
#             super(channel);
#             this.blockingStub = TypeDBGrpc.newBlockingStub(channel);
#             this.asyncStub = TypeDBGrpc.newStub(channel);
#         }
#
#         public CoreDatabaseManager.Contains.Res databasesContains(CoreDatabaseManager.Contains.Req request) {
#             return resilientCall(() -> blockingStub.databasesContains(request));
#         }
#
#         public CoreDatabaseManager.Create.Res databasesCreate(CoreDatabaseManager.Create.Req request) {
#             return resilientCall(() -> blockingStub.databasesCreate(request));
#         }
#
#         public CoreDatabaseManager.All.Res databasesAll(CoreDatabaseManager.All.Req request) {
#             return resilientCall(() -> blockingStub.databasesAll(request));
#         }
#
#         public CoreDatabase.Schema.Res databaseSchema(CoreDatabase.Schema.Req request) {
#             return resilientCall(() -> blockingStub.databaseSchema(request));
#         }
#
#         public CoreDatabase.Delete.Res databaseDelete(CoreDatabase.Delete.Req request) {
#             return resilientCall(() -> blockingStub.databaseDelete(request));
#         }
#
#         public Session.Open.Res sessionOpen(Session.Open.Req request) {
#             return resilientCall(() -> blockingStub.sessionOpen(request));
#         }
#
#         public Session.Close.Res sessionClose(Session.Close.Req request) {
#             return resilientCall(() -> blockingStub.sessionClose(request));
#         }
#
#         public Session.Pulse.Res sessionPulse(Session.Pulse.Req request) {
#             return resilientCall(() -> blockingStub.sessionPulse(request));
#         }
#
#         public StreamObserver<TransactionProto.Transaction.Client> transaction(StreamObserver<TransactionProto.Transaction.Server> responseObserver) {
#             return resilientCall(() -> asyncStub.transaction(responseObserver));
#         }
#     }
#
#     public static class Cluster extends TypeDBStub {
#
#         private final TypeDBClusterGrpc.TypeDBClusterBlockingStub blockingStub;
#
#         private Cluster(ManagedChannel channel) {
#             super(channel);
#             this.blockingStub = TypeDBClusterGrpc.newBlockingStub(channel);
#         }
#
#         public ClusterServerProto.ServerManager.All.Res serversAll(ClusterServerProto.ServerManager.All.Req request) {
#             return resilientCall(() -> blockingStub.serversAll(request));
#         }
#
#         public ClusterDatabaseManager.Get.Res databasesGet(ClusterDatabaseManager.Get.Req request) {
#             return resilientCall(() -> blockingStub.databasesGet(request));
#         }
#
#         public ClusterDatabaseManager.All.Res databasesAll(ClusterDatabaseManager.All.Req request) {
#             return resilientCall(() -> blockingStub.databasesAll(request));
#         }
#     }
# }
