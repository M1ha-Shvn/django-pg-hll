"""
django-pg-bulk-update support.
"""

from django.db.models.sql import Query

from .compatibility import django_pg_bulk_update_available
from .fields import HllField
from .values import HllEmpty, HllValue, HllCombinedExpression

# As django-pg-bulk-update library is not required, import only if it exists
if django_pg_bulk_update_available():
    from django_pg_bulk_update.set_functions import ConcatSetFunction
    from django_pg_bulk_update.compatibility import get_field_db_type
else:
    class ConcatSetFunction:
        pass

    def get_field_db_type(field, conn):
        raise NotImplementedError


class HllConcatFunction(ConcatSetFunction):
    names = {'hll_concat'}
    supported_field_classes = {'HllField'}

    def _parse_null_default(self, field, connection, **kwargs):
        kwargs['null_default'] = kwargs.get('null_default', HllEmpty())
        return super(HllConcatFunction, self)._parse_null_default(field, connection, **kwargs)

    def format_field_value(self, field, val, connection, cast_type=False, **kwargs):
        if not isinstance(field, HllField):
            return super(HllConcatFunction, self).format_field_value(field, val, connection, cast_type=cast_type,
                                                                     **kwargs)

        if not isinstance(val, (HllValue, HllCombinedExpression)):
            raise ValueError('val should be HllValue instance')

        compiler = Query(field.model).get_compiler(connection=connection)
        sql, params = val.as_sql(compiler, connection)

        if cast_type:
            sql = 'CAST(%s AS %s)' % (sql, get_field_db_type(field, connection))

        return sql, tuple(params)
