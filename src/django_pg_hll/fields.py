"""
This file contains a field to use in django models
"""
from django.core.exceptions import ValidationError
from django.db.models import Field, BinaryField

from .values import HllSet, HllValue, HllEmpty


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
        self._log2m = kwargs.get('log2m', self.custom_params['log2m'])
        self._regwidth= kwargs.get('regwidth', self.custom_params['regwidth'])
        self._expthresh = kwargs.get('expthresh', self.custom_params['expthresh'])
        self._sparseon = kwargs.get('sparseon', self.custom_params['sparseon'])

        super(HllField, self).__init__(*args, **kwargs)

    def deconstruct(self):
        name, path, args, kwargs = super().deconstruct()

        # Only include kwarg if it's not the default
        for param_name, default in self.custom_params.items():
            if getattr(self, '_%s' % name) != default:
                kwargs[name] = getattr(self, '_%s' % name)

        return name, path, args, kwargs

    def db_type(self, connection):
        return 'hll(%d, %d, %d, %d)' % (self._log2m, self._regwidth, self._expthresh, self._sparseon)

    def get_internal_type(self):
        return self.__class__.__name__

    def get_default(self):
        if self.has_default() and not callable(self.default):
            return self.default
        default = super().get_default()
        if default == '':
            return HllEmpty()
        return default

    def get_db_prep_value(self, value, connection, prepared=False):
        value = super().get_db_prep_value(value, connection, prepared)
        if value is not None:
            return connection.Database.Binary(value)

        if isinstance(value, HllValue):
            sql, params = value.get_sql(connection)

        return params

from django.db.models import F