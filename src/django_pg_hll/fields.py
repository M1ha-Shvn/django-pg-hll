"""
This file contains a field to use in django models
"""
from django.db.models import BinaryField, IntegerField
from django.db.models.lookups import Transform

import six

from .values import HllEmpty, HllFromHex

__all__ = ['HllField']


class HllField(BinaryField):
    """
    A field, wrapping hll
    """
    description = "Postgres HyperLogLog"
    empty_values = [None, b'', HllEmpty()]

    # Default values were taken from https://github.com/citusdata/postgresql-hll#defaults
    custom_params = {
        'log2m': 11,
        'regwidth': 5,
        'expthresh': -1,
        'sparseon': 1
    }

    def __init__(self, *args, **kwargs):
        self._log2m = kwargs.pop('log2m', self.custom_params['log2m'])
        self._regwidth = kwargs.pop('regwidth', self.custom_params['regwidth'])
        self._expthresh = kwargs.pop('expthresh', self.custom_params['expthresh'])
        self._sparseon = kwargs.pop('sparseon', self.custom_params['sparseon'])

        super(HllField, self).__init__(*args, **kwargs)

    def deconstruct(self):
        name, path, args, kwargs = super(HllField, self).deconstruct()

        # Only include kwarg if it's not the default
        for param_name, default in self.custom_params.items():
            if getattr(self, '_%s' % param_name) != default:
                kwargs[param_name] = getattr(self, '_%s' % param_name)

        return name, path, args, kwargs

    def db_type(self, connection):
        return 'hll(%d, %d, %d, %d)' % (self._log2m, self._regwidth, self._expthresh, self._sparseon)

    def rel_db_type(self, connection):
        return 'hll'

    def get_internal_type(self):
        return self.__class__.__name__

    def get_db_prep_value(self, value, connection, prepared=False):
        # Psycopg2 returns Binary results as hex string, prefixed by \x
        # BinaryField requires bytes to be saved
        # But none of these can be converted to HLL by postgres directly
        if isinstance(value, bytes) or isinstance(value, six.string_types) and value.startswith(r'\x'):
            return HllFromHex(value, db_type=self.db_type(connection))
        else:
            return super(HllField, self).get_db_prep_value(value, connection, prepared=prepared)

    def get_default(self):
        if self.has_default() and not callable(self.default):
            return self.default
        default = super(HllField, self).get_default()
        if default == '':
            return HllEmpty()
        return default


@HllField.register_lookup
class CardinalityTransform(Transform):
    lookup_name = 'cardinality'
    output_field = IntegerField()

    def as_sql(self, compiler, connection, function=None, template=None):
        lhs, params = compiler.compile(self.lhs)
        return 'hll_cardinality(%s)' % lhs, params
