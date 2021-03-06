# This file is a part of TypeDBClient.  License is MIT: https://github.com/Humans-of-Julia/TypeDBClient.jl/blob/main/LICENSE 

using ExecutableSpecifications.Gherkin: Given, When, Then
using AttributeType
using TypeDBClientException
using ThingSteps: get, put

###### When Steps ######################
#                                      #
########################################


@when("\$x = attribute(is-alive) as(boolean) put: true") do context
    @fail "Implement me"
end

#maybe like this? :D
@when("{var:Var} = attribute({type_label}) as(boolean) put: {value:Bool}") do context
    context[Symbol(var)] = [:tx(), :concepts(), get_attribute_type(:type_label::String), :as_remote(context[:tx()]), :as_boolean(), :put(value::bool)]
end

#=
@step("{var:Var} = attribute({type_label}) as(boolean) put: {value:Bool}")
def step_impl(context: Context, var: str, type_label: str, value: bool):
    context.put(var, context.tx().concepts().get_attribute_type(type_label).as_remote(context.tx()).as_boolean().put(value))
=#


@when("\$x = attribute(age) as(long) put: 21") do context
    @fail "Implement me"
end


@when("\$x = attribute(score) as(double) put: 123.456") do context
    @fail "Implement me"
end


@when("\$x = attribute(name) as(string) put: alice") do context
    @fail "Implement me"
end


@when("\$x = attribute(email) as(string) put: alice@email.com") do context
    @fail "Implement me"
end


@when("attribute(email) as(string) put: alice-email-com; throws exception") do context
    @fail "Implement me"
end


@when("\$x = attribute(birth-date) as(datetime) put: 1990-01-01 11:22:33") do context
    @fail "Implement me"
end


@when("\$x = attribute(score) as(double) put: 123") do context
    @fail "Implement me"
end


@when("delete attribute: \$x") do context
    @fail "Implement me"
end

###### Then Steps ######################
#                                      #
########################################

@then("attribute \$x is null: false") do context
    @fail "Implement me"
end


@then("attribute \$x has type: is-alive") do context
    @fail "Implement me"
end


@then("attribute \$x has value type: boolean") do context
    @fail "Implement me"
end


@then("attribute \$x has boolean value: true") do context
    @fail "Implement me"
end


@then("attribute \$x has type: age") do context
    @fail "Implement me"
end


@then("attribute \$x has value type: long") do context
    @fail "Implement me"
end


@then("attribute \$x has long value: 21") do context
    @fail "Implement me"
end


@then("attribute \$x has type: score") do context
    @fail "Implement me"
end


@then("attribute \$x has value type: double") do context
    @fail "Implement me"
end


@then("attribute \$x has double value: 123.456") do context
    @fail "Implement me"
end


@then("attribute \$x has type: name") do context
    @fail "Implement me"
end


@then("attribute \$x has value type: string") do context
    @fail "Implement me"
end


@then("attribute \$x has string value: alice") do context
    @fail "Implement me"
end


@then("attribute \$x has type: email") do context
    @fail "Implement me"
end


@then("attribute \$x has string value: alice@email.com") do context
    @fail "Implement me"
end


@then("attribute \$x has type: birth-date") do context
    @fail "Implement me"
end


@then("attribute \$x has value type: datetime") do context
    @fail "Implement me"
end


@then("attribute \$x has datetime value: 1990-01-01 11:22:33") do context
    @fail "Implement me"
end


@then("attribute(is-alive) get instances contain: \$x") do context
    @fail "Implement me"
end


@then("attribute(age) get instances contain: \$x") do context
    @fail "Implement me"
end


@then("attribute(score) get instances contain: \$x") do context
    @fail "Implement me"
end


@then("attribute(name) get instances contain: \$x") do context
    @fail "Implement me"
end


@then("attribute(birth-date) get instances contain: \$x") do context
    @fail "Implement me"
end


@then("attribute \$x is deleted: true") do context
    @fail "Implement me"
end


@then("attribute \$x is null: true") do context
    @fail "Implement me"
end


@then("attribute \$x has double value: 123") do context
    @fail "Implement me"
end







#=
from datetime import datetime

from behave import *
from hamcrest import *

from typedb.api.concept.type.attribute_type import AttributeType
from typedb.common.exception import TypeDBClientException
from tests.behaviour.context import Context


@step("attribute({type_label}) get instances contain: {var:Var}")
def step_impl(context: Context, type_label: str, var: str):
    assert_that(context.get(var), is_in(context.tx().concepts().get_attribute_type(type_label).as_remote(context.tx()).get_instances()))


@step("attribute({type_label}) get instances is empty")
def step_impl(context: Context, type_label: str):
    assert_that(calling(next).with_args(context.tx().concepts().get_attribute_type(type_label).as_remote(context.tx()).get_instances()), raises(StopIteration))


@step("attribute {var1:Var} get owners contain: {var2:Var}")
def step_impl(context: Context, var1: str, var2: str):
    assert_that(context.get(var2), is_in(context.get(var1).as_remote(context.tx()).get_owners()))


@step("attribute {var1:Var} get owners do not contain: {var2:Var}")
def step_impl(context: Context, var1: str, var2: str):
    assert_that(context.get(var2), not_(is_in(context.get(var1).as_remote(context.tx()).get_owners())))


@step("attribute {var:Var} has value type: {value_type:ValueType}")
def step_impl(context: Context, var: str, value_type: AttributeType.ValueType):
    assert_that(context.get(var).get_type().get_value_type(), is_(value_type))


@step("attribute({type_label}) as(boolean) put: {value:Bool}; throws exception")
def step_impl(context: Context, type_label: str, value: bool):
    assert_that(calling(context.tx().concepts().get_attribute_type(type_label).as_remote(context.tx()).as_boolean().put).with_args(value), raises(TypeDBClientException))


@step("{var:Var} = attribute({type_label}) as(boolean) put: {value:Bool}")
def step_impl(context: Context, var: str, type_label: str, value: bool):
    context.put(var, context.tx().concepts().get_attribute_type(type_label).as_remote(context.tx()).as_boolean().put(value))


@step("attribute({type_label}) as(long) put: {value:Int}; throws exception")
def step_impl(context: Context, type_label: str, value: int):
    assert_that(calling(context.tx().concepts().get_attribute_type(type_label).as_remote(context.tx()).as_long().put).with_args(value), raises(TypeDBClientException))


@step("{var:Var} = attribute({type_label}) as(long) put: {value:Int}")
def step_impl(context: Context, var: str, type_label: str, value: int):
    context.put(var, context.tx().concepts().get_attribute_type(type_label).as_remote(context.tx()).as_long().put(value))


@step("attribute({type_label}) as(double) put: {value:Float}; throws exception")
def step_impl(context: Context, type_label: str, value: float):
    assert_that(calling(context.tx().concepts().get_attribute_type(type_label).as_remote(context.tx()).as_double().put).with_args(value), raises(TypeDBClientException))


@step("{var:Var} = attribute({type_label}) as(double) put: {value:Float}")
def step_impl(context: Context, var: str, type_label: str, value: float):
    context.put(var, context.tx().concepts().get_attribute_type(type_label).as_remote(context.tx()).as_double().put(value))


@step("attribute({type_label}) as(string) put: {value}; throws exception")
def step_impl(context: Context, type_label: str, value: str):
    assert_that(calling(context.tx().concepts().get_attribute_type(type_label).as_remote(context.tx()).as_string().put).with_args(value), raises(TypeDBClientException))


@step("{var:Var} = attribute({type_label}) as(string) put: {value}")
def step_impl(context: Context, var: str, type_label: str, value: str):
    context.put(var, context.tx().concepts().get_attribute_type(type_label).as_remote(context.tx()).as_string().put(value))


@step("attribute({type_label}) as(datetime) put: {value:DateTime}; throws exception")
def step_impl(context: Context, type_label: str, value: datetime):
    assert_that(calling(context.tx().concepts().get_attribute_type(type_label).as_remote(context.tx()).as_datetime().put).with_args(value), raises(TypeDBClientException))


@step("{var:Var} = attribute({type_label}) as(datetime) put: {value:DateTime}")
def step_impl(context: Context, var: str, type_label: str, value: datetime):
    context.put(var, context.tx().concepts().get_attribute_type(type_label).as_remote(context.tx()).as_datetime().put(value))


@step("{var:Var} = attribute({type_label}) as(boolean) get: {value:Bool}")
def step_impl(context: Context, var: str, type_label: str, value: bool):
    context.put(var, context.tx().concepts().get_attribute_type(type_label).as_remote(context.tx()).as_boolean().get(value))


@step("{var:Var} = attribute({type_label}) as(long) get: {value:Int}")
def step_impl(context: Context, var: str, type_label: str, value: int):
    context.put(var, context.tx().concepts().get_attribute_type(type_label).as_remote(context.tx()).as_long().get(value))


@step("{var:Var} = attribute({type_label}) as(double) get: {value:Float}")
def step_impl(context: Context, var: str, type_label: str, value: float):
    context.put(var, context.tx().concepts().get_attribute_type(type_label).as_remote(context.tx()).as_double().get(value))


@step("{var:Var} = attribute({type_label}) as(string) get: {value}")
def step_impl(context: Context, var: str, type_label: str, value: str):
    context.put(var, context.tx().concepts().get_attribute_type(type_label).as_remote(context.tx()).as_string().get(value))


@step("{var:Var} = attribute({type_label}) as(datetime) get: {value:DateTime}")
def step_impl(context: Context, var: str, type_label: str, value: datetime):
    context.put(var, context.tx().concepts().get_attribute_type(type_label).as_remote(context.tx()).as_datetime().get(value))


@step("attribute {var:Var} has boolean value: {value:Bool}")
def step_impl(context: Context, var: str, value: bool):
    assert_that(context.get(var).get_value(), is_(value))


@step("attribute {var:Var} has long value: {value:Int}")
def step_impl(context: Context, var: str, value: int):
    assert_that(context.get(var).get_value(), is_(value))


@step("attribute {var:Var} has double value: {value:Float}")
def step_impl(context: Context, var: str, value: float):
    assert_that(context.get(var).get_value(), is_(value))


@step("attribute {var:Var} has string value: {value}")
def step_impl(context: Context, var: str, value: str):
    assert_that(context.get(var).get_value(), is_(value))


@step("attribute {var:Var} has datetime value: {value:DateTime}")
def step_impl(context: Context, var: str, value: datetime):
    assert_that(context.get(var).get_value(), is_(value))
=#
