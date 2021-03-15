# This file is a part of GraknClient.  License is MIT: https://github.com/Humans-of-Julia/GraknClient.jl/blob/main/LICENSE 

# 
# package grakn.client.concept.type;
# 
# import grakn.client.api.Transaction;
# import grakn.client.api.concept.type.RoleType;
# import grakn.client.common.Proto;
# import grakn.protocol.ConceptProto;
# 
# import javax.annotation.Nullable;
# import java.util.Objects;
# import java.util.stream.Stream;
# 
# import static grakn.common.util.Objects.className;
# 
# public class RoleTypeImpl extends TypeImpl implements RoleType {
# 
#     private final String scope;
#     private final int hash;
# 
#     RoleTypeImpl(String label, String scope, boolean root) {
#         super(label, root);
#         this.scope = scope;
#         this.hash = Objects.hash(this.scope, label);
#     }
# 
#     public static RoleTypeImpl of(ConceptProto.Type typeProto) {
#         return new RoleTypeImpl(typeProto.getLabel(), typeProto.getScope(), typeProto.getRoot());
#     }
# 
#     public static ConceptProto.Type protoRoleTypes(RoleType roleType) {
#         return Proto.Type.RoleType.roleType(roleType.getScope(), roleType.getLabel(), TypeImpl.encoding(roleType));
#     }
# 
#     @Override
#     public final String getScope() {
#         return scope;
#     }
# 
#     @Override
#     public final String getScopedLabel() {
#         return scope + ":" + getLabel();
#     }
# 
#     @Override
#     public RoleTypeImpl.Remote asRemote(Transaction transaction) {
#         return new RoleTypeImpl.Remote(transaction, getLabel(), getScope(), isRoot());
#     }
# 
#     @Override
#     public RoleTypeImpl asRoleType() {
#         return this;
#     }
# 
#     @Override
#     public String toString() {
#         return className(this.getClass()) + "[label: " + (scope != null ? scope + ":" : "") + getLabel() + "]";
#     }
# 
#     @Override
#     public boolean equals(Object o) {
#         if (this == o) return true;
#         if (o == null || getClass() != o.getClass()) return false;
# 
#         RoleTypeImpl that = (RoleTypeImpl) o;
#         return (this.getLabel().equals(that.getLabel()) && Objects.equals(this.scope, that.scope));
#     }
# 
#     @Override
#     public int hashCode() {
#         return hash;
#     }
# 
#     public static class Remote extends TypeImpl.Remote implements RoleType.Remote {
# 
#         private final String scope;
#         private final int hash;
# 
#         public Remote(Transaction transaction, String label, String scope, boolean isRoot) {
#             super(transaction, label, isRoot);
#             this.scope = scope;
#             this.hash = Objects.hash(transaction, label, scope);
#         }
# 
#         @Override
#         public final String getScope() {
#             return scope;
#         }
# 
#         @Override
#         public final String getScopedLabel() {
#             return scope + ":" + getLabel();
#         }
# 
#         @Nullable
#         @Override
#         public RoleTypeImpl getSupertype() {
#             TypeImpl supertype = super.getSupertype();
#             return supertype != null ? supertype.asRoleType() : null;
#         }
# 
#         @Override
#         public final Stream<RoleTypeImpl> getSupertypes() {
#             return super.getSupertypes().map(TypeImpl::asRoleType);
#         }
# 
#         @Override
#         public final Stream<RoleTypeImpl> getSubtypes() {
#             return super.getSubtypes().map(TypeImpl::asRoleType);
#         }
# 
#         @Override
#         public RoleType.Remote asRemote(Transaction transaction) {
#             return new RoleTypeImpl.Remote(transaction, getLabel(), getScope(), isRoot());
#         }
# 
#         @Override
#         public final RelationTypeImpl getRelationType() {
#             ConceptProto.Type.Res res = execute(Proto.Type.RoleType.getRelationType(getScope(), getLabel()));
#             return RelationTypeImpl.of(res.getRoleTypeGetRelationTypeRes().getRelationType());
#         }
# 
#         @Override
#         public final Stream<RelationTypeImpl> getRelationTypes() {
#             return stream(Proto.Type.RoleType.getRelationTypes(getScope(), getLabel()))
#                     .flatMap(rp -> rp.getRoleTypeGetRelationTypesResPart().getRelationTypesList().stream())
#                     .map(RelationTypeImpl::of);
#         }
# 
#         @Override
#         public final Stream<ThingTypeImpl> getPlayers() {
#             return stream(Proto.Type.RoleType.getPlayers(getScope(), getLabel()))
#                     .flatMap(rp -> rp.getRoleTypeGetPlayersResPart().getThingTypesList().stream())
#                     .map(ThingTypeImpl::of);
#         }
# 
#         @Override
#         public RoleTypeImpl.Remote asRoleType() {
#             return this;
#         }
# 
#         @Override
#         public String toString() {
#             return className(this.getClass()) + "[label: " + scope + ":" + getLabel() + "]";
#         }
# 
#         @Override
#         public boolean equals(Object o) {
#             if (this == o) return true;
#             if (o == null || getClass() != o.getClass()) return false;
# 
#             RoleTypeImpl.Remote that = (RoleTypeImpl.Remote) o;
#             return (this.tx().equals(that.tx()) &&
#                     this.getLabel().equals(that.getLabel()) &&
#                     Objects.equals(this.getScope(), that.getScope()));
#         }
# 
#         @Override
#         public int hashCode() {
#             return hash;
#         }
#     }
# }
