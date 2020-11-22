from django.db.models import Aggregate, IntegerField

from .fields import HllField


class Cardinality(Aggregate):
    function = 'hll_cardinality'
    output_field = IntegerField()


class UnionAgg(Aggregate):
    function = 'hll_union_agg'
    output_field = HllField()


class UnionAggCardinality(Aggregate):
    """
    I haven't found a way to combine function inside function in django.
    So, I've written function to get aggregate cardinality with one call
    """
    function = 'hll_union_agg'
    template = 'hll_cardinality(%(function)s(%(expressions)s))'
    output_field = IntegerField()


class CardinalitySum(Aggregate):
    """
    I haven't found a way to combine function inside function in django.
    So, I've written function to get sum cardinality with one call
    """
    function = 'hll_cardinality'
    template = 'SUM(%(function)s(%(expressions)s))'
    output_field = IntegerField()
