"""
This file contains a field to use in django models
"""
from django.db.models import FloatField, Transform, IntegerField

from .fields import HllField


class BaseHllTransformMixin:
    output_field = IntegerField()

    @property
    def function(self):
        return 'hll_%s' % self.lookup_name


@HllField.register_lookup
class CardinalityTransform(BaseHllTransformMixin, Transform):
    lookup_name = 'cardinality'
    output_field = FloatField()


@HllField.register_lookup
class SchemaVersionTransform(BaseHllTransformMixin, Transform):
    lookup_name = 'schema_version'


@HllField.register_lookup
class TypeTransform(BaseHllTransformMixin, Transform):
    lookup_name = 'type'


@HllField.register_lookup
class RegWidthTransform(BaseHllTransformMixin, Transform):
    lookup_name = 'regwidth'


@HllField.register_lookup
class Log2MTransform(BaseHllTransformMixin, Transform):
    lookup_name = 'log2m'


@HllField.register_lookup
class SParseOnTransform(BaseHllTransformMixin, Transform):
    lookup_name = 'sparseon'
