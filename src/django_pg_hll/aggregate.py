from django.db.models import Aggregate, IntegerField, FloatField

from .fields import ArrayFromTupleField, HllField


class Cardinality(Aggregate):
    function = 'hll_cardinality'
    output_field = FloatField()


class UnionAgg(Aggregate):
    function = 'hll_union_agg'
    output_field = HllField()


class HllConfigurationMixin:
    output_field = IntegerField()
    arity = 1


class HllSchemaVersion(HllConfigurationMixin, Aggregate):
    function = 'hll_schema_version'


class HllType(HllConfigurationMixin, Aggregate):
    function = 'hll_type'


class HllRegWidth(HllConfigurationMixin, Aggregate):
    function = 'hll_regwidth'


class HllLog2M(HllConfigurationMixin, Aggregate):
    function = 'hll_log2m'


class HllExpThreshold(HllConfigurationMixin, Aggregate):
    function = 'hll_expthresh'
    output_field = ArrayFromTupleField(IntegerField())


class HllSParseOn(HllConfigurationMixin, Aggregate):
    function = 'hll_sparseon'


class UnionAggCardinality(Aggregate):
    """
    I haven't found a way to combine function inside function in django.
    So, I've written function to get aggregate cardinality with one call
    """
    function = 'hll_union_agg'
    template = 'hll_cardinality(%(function)s(%(expressions)s))'
    output_field = FloatField()


class CardinalitySum(Aggregate):
    """
    I haven't found a way to combine function inside function in django.
    So, I've written function to get sum cardinality with one call
    """
    function = 'hll_cardinality'
    template = 'SUM(%(function)s(%(expressions)s))'
    output_field = FloatField()
