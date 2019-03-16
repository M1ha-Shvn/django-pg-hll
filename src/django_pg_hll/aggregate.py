from django.db.models import Aggregate, IntegerField

from django_pg_hll import HllField


class HllCardinality(Aggregate):
    function = 'hll_cardinality'
    output_field = IntegerField()


class HllUnionAgg(Aggregate):
    function = 'hll_union_agg'
    output_field = HllField()
