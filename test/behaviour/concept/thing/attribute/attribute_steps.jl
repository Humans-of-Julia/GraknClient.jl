# This file is a part of GraknClient.  License is MIT: https://github.com/Humans-of-Julia/GraknClient.jl/blob/main/LICENSE



#=
from datetime import datetime

from behave import *
from hamcrest import *

from grakn.common.exception import GraknClientException
from grakn.concept.type.value_type import ValueType
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
def step_impl(context: Context, var: str, value_type: ValueType):
    assert_that(context.get(var).get_type().get_value_type(), is_(value_type))


@step("attribute({type_label}) as(boolean) put: {value:Bool}; throws exception")
def step_impl(context: Context, type_label: str, value: bool):
    assert_that(calling(context.tx().concepts().get_attribute_type(type_label).as_remote(context.tx()).put).with_args(value), raises(GraknClientException))


@step("{var:Var} = attribute({type_label}) as(boolean) put: {value:Bool}")
def step_impl(context: Context, var: str, type_label: str, value: bool):
    context.put(var, context.tx().concepts().get_attribute_type(type_label).as_remote(context.tx()).put(value))


@step("attribute({type_label}) as(long) put: {value:Int}; throws exception")
def step_impl(context: Context, type_label: str, value: int):
    assert_that(calling(context.tx().concepts().get_attribute_type(type_label).as_remote(context.tx()).put).with_args(value), raises(GraknClientException))


@step("{var:Var} = attribute({type_label}) as(long) put: {value:Int}")
def step_impl(context: Context, var: str, type_label: str, value: int):
    context.put(var, context.tx().concepts().get_attribute_type(type_label).as_remote(context.tx()).put(value))


@step("attribute({type_label}) as(double) put: {value:Float}; throws exception")
def step_impl(context: Context, type_label: str, value: float):
    assert_that(calling(context.tx().concepts().get_attribute_type(type_label).as_remote(context.tx()).put).with_args(value), raises(GraknClientException))


@step("{var:Var} = attribute({type_label}) as(double) put: {value:Float}")
def step_impl(context: Context, var: str, type_label: str, value: float):
    context.put(var, context.tx().concepts().get_attribute_type(type_label).as_remote(context.tx()).put(value))


@step("attribute({type_label}) as(string) put: {value}; throws exception")
def step_impl(context: Context, type_label: str, value: str):
    assert_that(calling(context.tx().concepts().get_attribute_type(type_label).as_remote(context.tx()).put).with_args(value), raises(GraknClientException))


@step("{var:Var} = attribute({type_label}) as(string) put: {value}")
def step_impl(context: Context, var: str, type_label: str, value: str):
    context.put(var, context.tx().concepts().get_attribute_type(type_label).as_remote(context.tx()).put(value))


@step("attribute({type_label}) as(datetime) put: {value:DateTime}; throws exception")
def step_impl(context: Context, type_label: str, value: datetime):
    assert_that(calling(context.tx().concepts().get_attribute_type(type_label).as_remote(context.tx()).put).with_args(value), raises(GraknClientException))


@step("{var:Var} = attribute({type_label}) as(datetime) put: {value:DateTime}")
def step_impl(context: Context, var: str, type_label: str, value: datetime):
    context.put(var, context.tx().concepts().get_attribute_type(type_label).as_remote(context.tx()).put(value))


@step("{var:Var} = attribute({type_label}) as(boolean) get: {value:Bool}")
def step_impl(context: Context, var: str, type_label: str, value: bool):
    context.put(var, context.tx().concepts().get_attribute_type(type_label).as_remote(context.tx()).get(value))


@step("{var:Var} = attribute({type_label}) as(long) get: {value:Int}")
def step_impl(context: Context, var: str, type_label: str, value: int):
    context.put(var, context.tx().concepts().get_attribute_type(type_label).as_remote(context.tx()).get(value))


@step("{var:Var} = attribute({type_label}) as(double) get: {value:Float}")
def step_impl(context: Context, var: str, type_label: str, value: float):
    context.put(var, context.tx().concepts().get_attribute_type(type_label).as_remote(context.tx()).get(value))


@step("{var:Var} = attribute({type_label}) as(string) get: {value}")
def step_impl(context: Context, var: str, type_label: str, value: str):
    context.put(var, context.tx().concepts().get_attribute_type(type_label).as_remote(context.tx()).get(value))


@step("{var:Var} = attribute({type_label}) as(datetime) get: {value:DateTime}")
def step_impl(context: Context, var: str, type_label: str, value: datetime):
    context.put(var, context.tx().concepts().get_attribute_type(type_label).as_remote(context.tx()).get(value))


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