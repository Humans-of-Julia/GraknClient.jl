# This file is a part of TypeDBClient.  License is MIT: https://github.com/Humans-of-Julia/TypeDBClient.jl/blob/main/LICENSE 

# 
# package typedb.client.api.database;
# 
# import javax.annotation.CheckReturnValue;
# import java.util.List;
# 
# public interface DatabaseManager {
# 
#     @CheckReturnValue
#     Database get(String name);
# 
#     @CheckReturnValue
#     boolean contains(String name);
#     // TODO: Return type should be 'Database' but right now that would require 2 server calls in Cluster
# 
#     void create(String name);
# 
#     @CheckReturnValue
#     List<? extends Database> all();
# 
#     interface Cluster extends DatabaseManager {
# 
#         @Override
#         @CheckReturnValue
#         Database.Cluster get(String name);
# 
#         @Override
#         @CheckReturnValue
#         List<Database.Cluster> all();
#     }
# }
