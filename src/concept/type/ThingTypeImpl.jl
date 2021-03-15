# This file is a part of GraknClient.  License is MIT: https://github.com/Humans-of-Julia/GraknClient.jl/blob/main/LICENSE 

# 
# package grakn.client.concept.type;
# 
# import grakn.client.api.Transaction;
# import grakn.client.api.concept.type.AttributeType;
# import grakn.client.api.concept.type.AttributeType.ValueType;
# import grakn.client.api.concept.type.RoleType;
# import grakn.client.api.concept.type.ThingType;
# import grakn.client.common.GraknClientException;
# import grakn.client.common.Proto;
# import grakn.client.concept.thing.ThingImpl;
# import grakn.protocol.ConceptProto;
# 
# import java.util.stream.Stream;
# 
# import static grakn.client.common.ErrorMessage.Concept.BAD_ENCODING;
# import static grakn.client.concept.type.RoleTypeImpl.protoRoleTypes;
# 
# public class ThingTypeImpl extends TypeImpl implements ThingType {
# 
#     ThingTypeImpl(String label, boolean isRoot) {
#         super(label, isRoot);
#     }
# 
#     public static ThingTypeImpl of(ConceptProto.Type typeProto) {
#         switch (typeProto.getEncoding()) {
#             case ENTITY_TYPE:
#                 return EntityTypeImpl.of(typeProto);
#             case RELATION_TYPE:
#                 return RelationTypeImpl.of(typeProto);
#             case ATTRIBUTE_TYPE:
#                 return AttributeTypeImpl.of(typeProto);
#             case THING_TYPE:
#                 assert typeProto.getRoot();
#                 return new ThingTypeImpl(typeProto.getLabel(), typeProto.getRoot());
#             case UNRECOGNIZED:
#             default:
#                 throw new GraknClientException(BAD_ENCODING.message(typeProto.getEncoding()));
#         }
#     }
# 
#     public static ConceptProto.Type protoThingType(ThingType thingType) {
#         return Proto.Type.ThingType.thingType(thingType.getLabel(), TypeImpl.encoding(thingType));
#     }
# 
#     @Override
#     public ThingTypeImpl.Remote asRemote(Transaction transaction) {
#         return new ThingTypeImpl.Remote(transaction, getLabel(), isRoot());
#     }
# 
#     @Override
#     public final ThingTypeImpl asThingType() {
#         return this;
#     }
# 
#     public static class Remote extends TypeImpl.Remote implements ThingType.Remote {
# 
#         Remote(Transaction transaction, String label, boolean isRoot) {
#             super(transaction, label, isRoot);
#         }
# 
#         void setSupertype(ThingType thingType) {
#             execute(Proto.Type.ThingType.setSupertype(getLabel(), protoThingType(thingType)));
#         }
# 
#         @Override
#         public ThingTypeImpl getSupertype() {
#             TypeImpl supertype = super.getSupertype();
#             return supertype != null ? supertype.asThingType() : null;
#         }
# 
#         @Override
#         public Stream<? extends ThingTypeImpl> getSupertypes() {
#             Stream<? extends TypeImpl> supertypes = super.getSupertypes();
#             return supertypes.map(TypeImpl::asThingType);
#         }
# 
#         @Override
#         public Stream<? extends ThingTypeImpl> getSubtypes() {
#             return super.getSubtypes().map(TypeImpl::asThingType);
#         }
# 
#         @Override
#         public Stream<? extends ThingImpl> getInstances() {
#             return stream(Proto.Type.ThingType.getInstances(getLabel()))
#                     .flatMap(rp -> rp.getThingTypeGetInstancesResPart().getThingsList().stream())
#                     .map(ThingImpl::of);
#         }
# 
#         @Override
#         public final void setAbstract() {
#             execute(Proto.Type.ThingType.setAbstract(getLabel()));
#         }
# 
#         @Override
#         public final void unsetAbstract() {
#             execute(Proto.Type.ThingType.unsetAbstract(getLabel()));
#         }
# 
#         @Override
#         public final void setPlays(RoleType roleType) {
#             execute(Proto.Type.ThingType.setPlays(getLabel(), protoRoleTypes(roleType)));
#         }
# 
#         @Override
#         public final void setPlays(RoleType roleType, RoleType overriddenRoleType) {
#             execute(Proto.Type.ThingType.setPlays(
#                     getLabel(), protoRoleTypes(roleType), protoRoleTypes(overriddenRoleType)
#             ));
#         }
# 
#         @Override
#         public void setOwns(AttributeType attributeType) {
#             setOwns(attributeType, false);
#         }
# 
#         @Override
#         public void setOwns(AttributeType attributeType, boolean isKey) {
#             execute(Proto.Type.ThingType.setOwns(getLabel(), protoThingType(attributeType), isKey));
#         }
# 
#         @Override
#         public void setOwns(AttributeType attributeType, AttributeType overriddenType) {
#             setOwns(attributeType, overriddenType, false);
#         }
# 
#         @Override
#         public final void setOwns(AttributeType attributeType, AttributeType overriddenType, boolean isKey) {
#             execute(Proto.Type.ThingType.setOwns(
#                     getLabel(), protoThingType(attributeType), protoThingType(overriddenType), isKey
#             ));
#         }
# 
#         @Override
#         public final Stream<RoleTypeImpl> getPlays() {
#             return stream(Proto.Type.ThingType.getPlays(getLabel()))
#                     .flatMap(rp -> rp.getThingTypeGetPlaysResPart().getRolesList().stream())
#                     .map(RoleTypeImpl::of);
#         }
# 
#         @Override
#         public Stream<AttributeTypeImpl> getOwns() {
#             return getOwns(null, false);
#         }
# 
#         @Override
#         public Stream<AttributeTypeImpl> getOwns(ValueType valueType) {
#             return getOwns(valueType, false);
#         }
# 
#         @Override
#         public Stream<AttributeTypeImpl> getOwns(boolean keysOnly) {
#             return stream(Proto.Type.ThingType.getOwns(getLabel(), keysOnly))
#                     .flatMap(rp -> rp.getThingTypeGetOwnsResPart().getAttributeTypesList().stream())
#                     .map(AttributeTypeImpl::of);
#         }
# 
#         @Override
#         public final Stream<AttributeTypeImpl> getOwns(ValueType valueType, boolean keysOnly) {
#             return stream(Proto.Type.ThingType.getOwns(getLabel(), valueType.proto(), keysOnly))
#                     .flatMap(rp -> rp.getThingTypeGetOwnsResPart().getAttributeTypesList().stream())
#                     .map(AttributeTypeImpl::of);
#         }
# 
#         @Override
#         public final void unsetPlays(RoleType roleType) {
#             execute(Proto.Type.ThingType.unsetPlays(getLabel(), protoRoleTypes(roleType)));
#         }
# 
#         @Override
#         public final void unsetOwns(AttributeType attributeType) {
#             execute(Proto.Type.ThingType.unsetOwns(getLabel(), protoThingType(attributeType)));
#         }
# 
#         @Override
#         public ThingTypeImpl.Remote asRemote(Transaction transaction) {
#             return new ThingTypeImpl.Remote(transaction, getLabel(), isRoot());
#         }
# 
#         @Override
#         public final ThingTypeImpl.Remote asThingType() {
#             return this;
#         }
#     }
# }
