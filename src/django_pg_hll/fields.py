"""
This file contains a field to use in django models
"""
import re
from django.contrib.postgres.fields import ArrayField
from django.db.models import BinaryField

from .compatibility import string_types
from .values import HllEmpty, HllFromHex

__all__ = ['HllField']


class HllField(BinaryField):
    """
    A field, wrapping hll
    """
    HLL_ARGS = ('log2m', 'regwidth', 'expthresh', 'sparseon')

    description = "Postgres HyperLogLog"
    empty_values = [None, b'', HllEmpty()]

    def __init__(self, *args, **kwargs):
        self.hll_arg_params = []
        all_args_found = True

        # Check that argument order has all arguments required for field creation
        for i, arg in enumerate(self.HLL_ARGS):
            if arg in kwargs:
                if not all_args_found:
                    raise ValueError('`%s` argument can be set only if [%s] arguments are set'
                                     % (arg, ', '.join(self.HLL_ARGS[:i])))
                else:
                    self.hll_arg_params.append(kwargs.pop(arg))
            else:
                all_args_found = False

        super(HllField, self).__init__(*args, **kwargs)

    def deconstruct(self):
        name, path, args, kwargs = super(HllField, self).deconstruct()

        for param_name, val in zip(self.HLL_ARGS, self.hll_arg_params):
            kwargs[param_name] = val

        return name, path, args, kwargs

    def db_type(self, connection):
        return ('hll(%s)' % ", ".join(str(val) for val in self.hll_arg_params)) if self.hll_arg_params else 'hll'

    def rel_db_type(self, connection):
        return 'hll'

    def get_internal_type(self):
        return self.__class__.__name__

    def get_db_prep_value(self, value, connection, prepared=False):
        # Psycopg2 returns Binary results as hex string, prefixed by \x
        # BinaryField requires bytes to be saved
        # But none of these can be converted to HLL by postgres directly
        if isinstance(value, bytes) or isinstance(value, string_types) and value.startswith(r'\x'):
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


class ArrayFromTupleField(ArrayField):
    """
    This field is used to return HllExpThresh result
    """
    def to_python(self, value):
        if isinstance(value, str):
            if not re.match(r'(.*)', value):
                raise ValueError('Tuple should be surrounded by braces')

            tuple_items = value[1:-1].split(',')
            value = [self.base_field.to_python(val) for val in tuple_items]

        return value

    def from_db_value(self, value, expression, connection, query_context=None):
        # query_context has been used in django < 2.0
        if value is None:
            return value

        if isinstance(value, str):
            value = self.to_python(value)

        return [self.base_field.from_db_value(item, expression, connection) for item in value] \
            if hasattr(self.base_field, 'from_db_value') else value
