# This file is a part of GraknClient.  License is MIT: https://github.com/Humans-of-Julia/GraknClient.jl/blob/main/LICENSE 

# 
# package grakn.client.cluster;
# 
# import grakn.client.api.Client;
# import grakn.client.api.GraknOptions;
# import grakn.client.api.Session;
# import grakn.client.common.GraknClientException;
# import grakn.client.core.CoreClient;
# import grakn.common.collection.Pair;
# import grakn.protocol.cluster.ClusterProto;
# import grakn.protocol.cluster.GraknClusterGrpc;
# import io.grpc.StatusRuntimeException;
# import org.slf4j.Logger;
# import org.slf4j.LoggerFactory;
# 
# import java.util.HashSet;
# import java.util.Map;
# import java.util.Set;
# import java.util.concurrent.ConcurrentHashMap;
# import java.util.concurrent.ConcurrentMap;
# import java.util.stream.Collectors;
# 
# import static grakn.client.common.ErrorMessage.Client.CLUSTER_UNABLE_TO_CONNECT;
# import static grakn.common.collection.Collections.pair;
# 
# public class ClusterClient implements Client.Cluster {
#     private static final Logger LOG = LoggerFactory.getLogger(ClusterClient.class);
#     private final Map<String, CoreClient> coreClients;
#     private final Map<String, GraknClusterGrpc.GraknClusterBlockingStub> graknClusterRPCs;
#     private final ClusterDatabaseManager databaseManagers;
#     private final ConcurrentMap<String, ClusterDatabase> clusterDatabases;
#     private boolean isOpen;
# 
#     public ClusterClient(Set<String> addresses) {
#         this(addresses, CoreClient.calculateParallelisation());
#     }
# 
#     public ClusterClient(Set<String> addresses, int parallelisation) {
#         coreClients = fetchClusterServers(addresses).stream().map(
#                 address -> pair(address, new CoreClient(address, parallelisation))
#         ).collect(Collectors.toMap(Pair::first, Pair::second));
#         graknClusterRPCs = coreClients.entrySet().stream().map(
#                 client -> pair(client.getKey(), GraknClusterGrpc.newBlockingStub(client.getValue().channel()))
#         ).collect(Collectors.toMap(Pair::first, Pair::second));
#         databaseManagers = new ClusterDatabaseManager(this, coreClients.entrySet().stream().map(
#                 client -> pair(client.getKey(), client.getValue().databases())
#         ).collect(Collectors.toMap(Pair::first, Pair::second)));
#         clusterDatabases = new ConcurrentHashMap<>();
#         isOpen = true;
#     }
# 
#     @Override
#     public boolean isOpen() {
#         return isOpen;
#     }
# 
#     @Override
#     public ClusterDatabaseManager databases() {
#         return databaseManagers;
#     }
# 
#     @Override
#     public ClusterSession session(String database, Session.Type type) {
#         return session(database, type, GraknOptions.cluster());
#     }
# 
#     @Override
#     public ClusterSession session(String database, Session.Type type, GraknOptions options) {
#         GraknOptions.Cluster clusterOptions = options.asCluster();
#         if (clusterOptions.readAnyReplica().isPresent() && clusterOptions.readAnyReplica().get()) {
#             return sessionAnyReplica(database, type, clusterOptions);
#         } else {
#             return sessionPrimaryReplica(database, type, clusterOptions);
#         }
#     }
# 
#     private ClusterSession sessionPrimaryReplica(
#             String database, Session.Type type, GraknOptions.Cluster options) {
#         return openSessionFailsafeTask(database, type, options, this).runPrimaryReplica();
#     }
# 
#     private ClusterSession sessionAnyReplica(
#             String database, Session.Type type, GraknOptions.Cluster options) {
#         return openSessionFailsafeTask(database, type, options, this).runAnyReplica();
#     }
# 
#     private FailsafeTask<ClusterSession> openSessionFailsafeTask(
#             String database, Session.Type type, GraknOptions.Cluster options, ClusterClient client) {
#         return new FailsafeTask<ClusterSession>(this, database) {
# 
#             @Override
#             ClusterSession run(ClusterDatabase.Replica replica) {
#                 return new ClusterSession(client, replica.address(), database, type, options);
#             }
#         };
#     }
# 
#     ConcurrentMap<String, ClusterDatabase> clusterDatabases() {
#         return clusterDatabases;
#     }
# 
#     public Set<String> clusterMembers() {
#         return coreClients.keySet();
#     }
# 
#     public CoreClient coreClient(String address) {
#         return coreClients.get(address);
#     }
# 
#     public GraknClusterGrpc.GraknClusterBlockingStub graknClusterRPC(String address) {
#         return graknClusterRPCs.get(address);
#     }
# 
#     private Set<String> fetchClusterServers(Set<String> addresses) {
#         for (String address : addresses) {
#             try (CoreClient client = new CoreClient(address)) {
#                 LOG.debug("Fetching list of cluster servers from {}...", address);
#                 GraknClusterGrpc.GraknClusterBlockingStub graknClusterRPC =
#                         GraknClusterGrpc.newBlockingStub(client.channel());
#                 ClusterProto.Cluster.Servers.Res res =
#                         graknClusterRPC.clusterServers(ClusterProto.Cluster.Servers.Req.newBuilder().build());
#                 Set<String> members = new HashSet<>(res.getAddressesList());
#                 LOG.debug("The cluster servers are {}", members);
#                 return members;
#             } catch (StatusRuntimeException e) {
#                 LOG.error("Fetching cluster servers from {} failed.", address);
#             }
#         }
#         throw new GraknClientException(CLUSTER_UNABLE_TO_CONNECT, String.join(",", addresses));
#     }
# 
#     @Override
#     public boolean isCluster() {
#         return true;
#     }
# 
#     @Override
#     public Cluster asCluster() {
#         return this;
#     }
# 
#     @Override
#     public void close() {
#         coreClients.values().forEach(CoreClient::close);
#         isOpen = false;
#     }
# }
