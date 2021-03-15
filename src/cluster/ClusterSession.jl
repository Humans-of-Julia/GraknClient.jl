# This file is a part of GraknClient.  License is MIT: https://github.com/Humans-of-Julia/GraknClient.jl/blob/main/LICENSE 

# 
# package grakn.client.cluster;
# 
# import grakn.client.api.GraknOptions;
# import grakn.client.api.Session;
# import grakn.client.api.Transaction;
# import grakn.client.api.database.Database;
# import grakn.client.core.CoreClient;
# import grakn.client.core.CoreSession;
# import org.slf4j.Logger;
# import org.slf4j.LoggerFactory;
# 
# public class ClusterSession implements Session {
#     private static final Logger LOG = LoggerFactory.getLogger(Session.class);
#     private final ClusterClient clusterClient;
#     private final GraknOptions.Cluster options;
#     private CoreClient coreClient;
#     private CoreSession coreSession;
# 
#     public ClusterSession(ClusterClient clusterClient, String serverAddress, String database, Type type, GraknOptions.Cluster options) {
#         this.clusterClient = clusterClient;
#         this.coreClient = clusterClient.coreClient(serverAddress);
#         LOG.debug("Opening a session to '{}'", serverAddress);
#         this.coreSession = coreClient.session(database, type, options);
#         this.options = options;
#     }
# 
#     @Override
#     public Transaction transaction(Transaction.Type type) {
#         return transaction(type, GraknOptions.cluster());
#     }
# 
#     @Override
#     public Transaction transaction(Transaction.Type type, GraknOptions options) {
#         GraknOptions.Cluster clusterOpt = options.asCluster();
#         if (clusterOpt.readAnyReplica().isPresent() && clusterOpt.readAnyReplica().get()) {
#             return transactionAnyReplica(type, clusterOpt);
#         } else {
#             return transactionPrimaryReplica(type, options);
#         }
#     }
# 
#     private Transaction transactionPrimaryReplica(Transaction.Type type, GraknOptions options) {
#         return transactionFailsafeTask(type, options).runPrimaryReplica();
#     }
# 
#     private Transaction transactionAnyReplica(Transaction.Type type, GraknOptions.Cluster options) {
#         return transactionFailsafeTask(type, options).runAnyReplica();
#     }
# 
#     private FailsafeTask<Transaction> transactionFailsafeTask(Transaction.Type type, GraknOptions options) {
#         return new FailsafeTask<Transaction>(clusterClient, database().name()) {
# 
#             @Override
#             Transaction run(ClusterDatabase.Replica replica) {
#                 return coreSession.transaction(type, options);
#             }
# 
#             @Override
#             Transaction rerun(ClusterDatabase.Replica replica) {
#                 if (coreSession != null) coreSession.close();
#                 coreClient = clusterClient.coreClient(replica.address());
#                 coreSession = coreClient.session(database().name(), ClusterSession.this.type(), ClusterSession.this.options());
#                 return coreSession.transaction(type, options);
#             }
#         };
#     }
# 
#     @Override
#     public Session.Type type() {
#         return coreSession.type();
#     }
# 
#     @Override
#     public GraknOptions.Cluster options() {
#         return options;
#     }
# 
#     @Override
#     public boolean isOpen() {
#         return coreSession.isOpen();
#     }
# 
#     @Override
#     public void close() {
#         coreSession.close();
#     }
# 
#     @Override
#     public Database database() {
#         return coreSession.database();
#     }
# }
