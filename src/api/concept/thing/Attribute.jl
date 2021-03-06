# This file is a part of TypeDBClient.  License is MIT: https://github.com/Humans-of-Julia/TypeDBClient.jl/blob/main/LICENSE 

# 
# package typedb.client.api.concept.thing;
# 
# import typedb.client.api.TypeDBTransaction;
# import typedb.client.api.concept.type.AttributeType;
# import typedb.client.api.concept.type.ThingType;
# 
# import javax.annotation.CheckReturnValue;
# import java.time.LocalDateTime;
# import java.util.stream.Stream;
# 
# public interface Attribute<VALUE> extends Thing {
# 
#     @Override
#     @CheckReturnValue
#     AttributeType getType();
# 
#     @CheckReturnValue
#     VALUE getValue();
# 
#     @Override
#     @CheckReturnValue
#     default boolean isAttribute() {
#         return true;
#     }
# 
#     @CheckReturnValue
#     default boolean isBoolean() {
#         return false;
#     }
# 
#     @CheckReturnValue
#     default boolean isLong() {
#         return false;
#     }
# 
#     @CheckReturnValue
#     default boolean isDouble() {
#         return false;
#     }
# 
#     @CheckReturnValue
#     default boolean isString() {
#         return false;
#     }
# 
#     @CheckReturnValue
#     default boolean isDateTime() {
#         return false;
#     }
# 
#     @CheckReturnValue
#     Attribute.Boolean asBoolean();
# 
#     @CheckReturnValue
#     Attribute.Long asLong();
# 
#     @CheckReturnValue
#     Attribute.Double asDouble();
# 
#     @CheckReturnValue
#     Attribute.String asString();
# 
#     @CheckReturnValue
#     Attribute.DateTime asDateTime();
# 
#     @Override
#     @CheckReturnValue
#     Attribute.Remote<VALUE> asRemote(TypeDBTransaction transaction);
# 
#     interface Remote<VALUE> extends Thing.Remote, Attribute<VALUE> {
# 
#         @CheckReturnValue
#         Stream<? extends Thing> getOwners();
# 
#         @CheckReturnValue
#         Stream<? extends Thing> getOwners(ThingType ownerType);
# 
#         @Override
#         @CheckReturnValue
#         Attribute.Remote<VALUE> asAttribute();
# 
#         @Override
#         @CheckReturnValue
#         Attribute.Boolean.Remote asBoolean();
# 
#         @Override
#         @CheckReturnValue
#         Attribute.Long.Remote asLong();
# 
#         @Override
#         @CheckReturnValue
#         Attribute.Double.Remote asDouble();
# 
#         @Override
#         @CheckReturnValue
#         Attribute.String.Remote asString();
# 
#         @Override
#         @CheckReturnValue
#         Attribute.DateTime.Remote asDateTime();
#     }
# 
#     interface Boolean extends Attribute<java.lang.Boolean> {
# 
#         @Override
#         @CheckReturnValue
#         default boolean isBoolean() {
#             return true;
#         }
# 
#         @Override
#         @CheckReturnValue
#         AttributeType.Boolean getType();
# 
#         @Override
#         @CheckReturnValue
#         Attribute.Boolean.Remote asRemote(TypeDBTransaction transaction);
# 
#         interface Remote extends Attribute.Boolean, Attribute.Remote<java.lang.Boolean> {}
#     }
# 
#     interface Long extends Attribute<java.lang.Long> {
# 
#         @Override
#         @CheckReturnValue
#         default boolean isLong() {
#             return true;
#         }
# 
#         @Override
#         @CheckReturnValue
#         AttributeType.Long getType();
# 
#         @Override
#         @CheckReturnValue
#         Attribute.Long.Remote asRemote(TypeDBTransaction transaction);
# 
#         interface Remote extends Attribute.Long, Attribute.Remote<java.lang.Long> {}
#     }
# 
#     interface Double extends Attribute<java.lang.Double> {
# 
#         @Override
#         @CheckReturnValue
#         default boolean isDouble() {
#             return true;
#         }
# 
#         @Override
#         @CheckReturnValue
#         AttributeType.Double getType();
# 
#         @Override
#         @CheckReturnValue
#         Attribute.Double.Remote asRemote(TypeDBTransaction transaction);
# 
#         interface Remote extends Attribute.Double, Attribute.Remote<java.lang.Double> {}
#     }
# 
#     interface String extends Attribute<java.lang.String> {
# 
#         @Override
#         @CheckReturnValue
#         default boolean isString() {
#             return true;
#         }
# 
#         @Override
#         @CheckReturnValue
#         AttributeType.String getType();
# 
#         @Override
#         @CheckReturnValue
#         Attribute.String.Remote asRemote(TypeDBTransaction transaction);
# 
#         interface Remote extends Attribute.String, Attribute.Remote<java.lang.String> {}
#     }
# 
#     interface DateTime extends Attribute<LocalDateTime> {
# 
#         @Override
#         @CheckReturnValue
#         default boolean isDateTime() {
#             return true;
#         }
# 
#         @Override
#         @CheckReturnValue
#         AttributeType.DateTime getType();
# 
#         @Override
#         @CheckReturnValue
#         Attribute.DateTime.Remote asRemote(TypeDBTransaction transaction);
# 
#         interface Remote extends Attribute.DateTime, Attribute.Remote<LocalDateTime> {}
#     }
# }
