# This file is a part of GraknClient.  License is MIT: https://github.com/Humans-of-Julia/GraknClient.jl/blob/main/LICENSE 

# 
# package grakn.client.api.concept.thing;
# 
# import grakn.client.api.Transaction;
# import grakn.client.api.concept.Concept;
# import grakn.client.api.concept.type.AttributeType;
# import grakn.client.api.concept.type.RoleType;
# import grakn.client.api.concept.type.ThingType;
# 
# import javax.annotation.CheckReturnValue;
# import java.util.stream.Stream;
# 
# public interface Thing extends Concept {
# 
#     @CheckReturnValue
#     String getIID();
# 
#     @CheckReturnValue
#     ThingType getType();
# 
#     @Override
#     @CheckReturnValue
#     default boolean isThing() {
#         return true;
#     }
# 
#     @Override
#     @CheckReturnValue
#     Thing.Remote asRemote(Transaction transaction);
# 
#     interface Remote extends Concept.Remote, Thing {
# 
#         void setHas(Attribute<?> attribute);
# 
#         void unsetHas(Attribute<?> attribute);
# 
#         @CheckReturnValue
#         boolean isInferred();
# 
#         @CheckReturnValue
#         Stream<? extends Attribute<?>> getHas(boolean onlyKey);
# 
#         @CheckReturnValue
#         Stream<? extends Attribute.Boolean> getHas(AttributeType.Boolean attributeType);
# 
#         @CheckReturnValue
#         Stream<? extends Attribute.Long> getHas(AttributeType.Long attributeType);
# 
#         @CheckReturnValue
#         Stream<? extends Attribute.Double> getHas(AttributeType.Double attributeType);
# 
#         @CheckReturnValue
#         Stream<? extends Attribute.String> getHas(AttributeType.String attributeType);
# 
#         @CheckReturnValue
#         Stream<? extends Attribute.DateTime> getHas(AttributeType.DateTime attributeType);
# 
#         @CheckReturnValue
#         Stream<? extends Attribute<?>> getHas(AttributeType... attributeTypes);
# 
#         @CheckReturnValue
#         Stream<? extends RoleType> getPlays();
# 
#         @CheckReturnValue
#         Stream<? extends Relation> getRelations(RoleType... roleTypes);
#     }
# }
