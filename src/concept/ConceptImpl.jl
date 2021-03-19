# This file is a part of GraknClient.  License is MIT: https://github.com/Humans-of-Julia/GraknClient.jl/blob/main/LICENSE 

# 
# package grakn.client.concept;
# 
# import grakn.client.api.concept.Concept;
# import grakn.client.api.concept.thing.Attribute;
# import grakn.client.api.concept.thing.Entity;
# import grakn.client.api.concept.thing.Relation;
# import grakn.client.api.concept.thing.Thing;
# import grakn.client.api.concept.type.AttributeType;
# import grakn.client.api.concept.type.EntityType;
# import grakn.client.api.concept.type.RelationType;
# import grakn.client.api.concept.type.RoleType;
# import grakn.client.api.concept.type.ThingType;
# import grakn.client.api.concept.type.Type;
# import grakn.client.common.GraknClientException;
# import grakn.client.concept.thing.AttributeImpl;
# import grakn.client.concept.thing.EntityImpl;
# import grakn.client.concept.thing.RelationImpl;
# import grakn.client.concept.thing.ThingImpl;
# import grakn.client.concept.type.AttributeTypeImpl;
# import grakn.client.concept.type.EntityTypeImpl;
# import grakn.client.concept.type.RelationTypeImpl;
# import grakn.client.concept.type.RoleTypeImpl;
# import grakn.client.concept.type.ThingTypeImpl;
# import grakn.client.concept.type.TypeImpl;
# import grakn.protocol.ConceptProto;
# 
# import static grakn.client.common.ErrorMessage.Concept.INVALID_CONCEPT_CASTING;
# import static grakn.common.util.Objects.className;
# 
# public abstract class ConceptImpl implements Concept {
# 
#     public static Concept of(ConceptProto.Concept owner) {
#         Concept concept;
#         if (owner.hasThing()) concept = ThingImpl.of(owner.getThing());
#         else concept = TypeImpl.of(owner.getType());
#         return concept;
#     }
# 
#     @Override
#     public final boolean isRemote() {
#         return false;
#     }
# 
#     @Override
#     public TypeImpl asType() {
#         throw new GraknClientException(INVALID_CONCEPT_CASTING, className(this.getClass()), className(Type.class));
#     }
# 
#     @Override
#     public ThingTypeImpl asThingType() {
#         throw new GraknClientException(INVALID_CONCEPT_CASTING, className(this.getClass()), className(ThingType.class));
#     }
# 
#     @Override
#     public EntityTypeImpl asEntityType() {
#         throw new GraknClientException(INVALID_CONCEPT_CASTING, className(this.getClass()), className(EntityType.class));
#     }
# 
#     @Override
#     public AttributeTypeImpl asAttributeType() {
#         throw new GraknClientException(INVALID_CONCEPT_CASTING, className(this.getClass()), className(AttributeType.class));
#     }
# 
#     @Override
#     public RelationTypeImpl asRelationType() {
#         throw new GraknClientException(INVALID_CONCEPT_CASTING, className(this.getClass()), className(RelationType.class));
#     }
# 
#     @Override
#     public RoleTypeImpl asRoleType() {
#         throw new GraknClientException(INVALID_CONCEPT_CASTING, className(this.getClass()), className(RoleType.class));
#     }
# 
#     @Override
#     public ThingImpl asThing() {
#         throw new GraknClientException(INVALID_CONCEPT_CASTING, className(this.getClass()), className(Thing.class));
#     }
# 
#     @Override
#     public EntityImpl asEntity() {
#         throw new GraknClientException(INVALID_CONCEPT_CASTING, className(this.getClass()), className(Entity.class));
#     }
# 
#     @Override
#     public AttributeImpl<?> asAttribute() {
#         throw new GraknClientException(INVALID_CONCEPT_CASTING, className(this.getClass()), className(Attribute.class));
#     }
# 
#     @Override
#     public RelationImpl asRelation() {
#         throw new GraknClientException(INVALID_CONCEPT_CASTING, className(this.getClass()), className(Relation.class));
#     }
# 
#     public abstract static class Remote implements Concept.Remote {
# 
#         @Override
#         public final boolean isRemote() {
#             return true;
#         }
# 
#         @Override
#         public TypeImpl.Remote asType() {
#             throw new GraknClientException(INVALID_CONCEPT_CASTING, className(this.getClass()), className(Type.class));
#         }
# 
#         @Override
#         public ThingTypeImpl.Remote asThingType() {
#             throw new GraknClientException(INVALID_CONCEPT_CASTING, className(this.getClass()), className(ThingType.class));
#         }
# 
#         @Override
#         public EntityTypeImpl.Remote asEntityType() {
#             throw new GraknClientException(INVALID_CONCEPT_CASTING, className(this.getClass()), className(EntityType.class));
#         }
# 
#         @Override
#         public RelationTypeImpl.Remote asRelationType() {
#             throw new GraknClientException(INVALID_CONCEPT_CASTING, className(this.getClass()), className(RelationType.class));
#         }
# 
#         @Override
#         public AttributeTypeImpl.Remote asAttributeType() {
#             throw new GraknClientException(INVALID_CONCEPT_CASTING, className(this.getClass()), className(AttributeType.class));
#         }
# 
#         @Override
#         public RoleTypeImpl.Remote asRoleType() {
#             throw new GraknClientException(INVALID_CONCEPT_CASTING, className(this.getClass()), className(RoleType.class));
#         }
# 
#         @Override
#         public ThingImpl.Remote asThing() {
#             throw new GraknClientException(INVALID_CONCEPT_CASTING, className(this.getClass()), className(Thing.class));
#         }
# 
#         @Override
#         public EntityImpl.Remote asEntity() {
#             throw new GraknClientException(INVALID_CONCEPT_CASTING, className(this.getClass()), className(Entity.class));
#         }
# 
#         @Override
#         public RelationImpl.Remote asRelation() {
#             throw new GraknClientException(INVALID_CONCEPT_CASTING, className(this.getClass()), className(Relation.class));
#         }
# 
#         @Override
#         public AttributeImpl.Remote<?> asAttribute() {
#             throw new GraknClientException(INVALID_CONCEPT_CASTING, className(this.getClass()), className(Attribute.class));
#         }
#     }
# }
