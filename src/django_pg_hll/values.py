from collections import Iterable
from copy import deepcopy
from itertools import chain
from typing import Any, Tuple, Set, Optional, Union

import six
from abc import abstractmethod, ABC
from django.contrib.postgres.fields import HStoreField
from django.db.backends.postgresql.base import DatabaseWrapper

from django_pg_hll.utils import get_subclasses


class HllValue(ABC):
    @abstractmethod
    def get_sql(self, connection):  # type: (DatabaseWrapper) -> Tuple[str, tuple]
        """
        Generates hashed sql expression for this value
        :param connection: Connection, sql is got for
        :return: A tuple with sql expression and parameters to substitute to it
        """
        raise NotImplementedError()


class HllEmpty(HllValue):
    def get_sql(self, connection):
        return 'hll_empty()', ()


class HllDataValue(HllValue):
    def __init__(self, data):  # type: (Any) -> None
        """
        :param data: Data to build value from
        """
        if self.check(data):
            self._data = data
        else:
            raise ValueError('Data is not supported by %s' % self.__class__.__name__)

    @classmethod
    @abstractmethod
    def check(cls, data):  # type: (Any) -> bool
        """
        Checks if this value type can be used for given data
        :param data: Data to check
        :return: True, if this value can be used
        """
        raise NotImplementedError()

    @classmethod
    def parse_data(cls, data):  # type: (Any) -> HllDataValue
        for klass in get_subclasses(HllDataValue, recursive=True):
            if klass.check(data):
                return klass(data)

        raise ValueError('No appropriate HllDataValue found')

    def get_db_prep_value(self, connection):  # type: (DatabaseWrapper) -> Any
        """
        Formats _data for passing to database
        :param connection: Connection, sql is got for
        :return: Formatted data
        """
        return self._data

    def __hash__(self):
        return hash(self._data)


class HllPrimitiveValue(HllDataValue, ABC):
    # Abstract class property
    db_type = None

    def __init__(self, data, **kwargs):  # type: (Any, **dict) -> None
        """
        :param data: Data to build value from
        :param hash_seed: Optional hash seed. See https://github.com/citusdata/postgresql-hll#the-importance-of-hashing
        """
        self._hash_seed = kwargs.pop('hash_seed', None)
        self._data = data
        super(HllPrimitiveValue, self).__init__()

    def get_sql(self, connection):
        if self._hash_seed is not None:
            return 'hll_hash_{0}(%s, %s)'.format(self.db_type), (self.get_db_prep_value(connection), self._hash_seed)
        else:
            return 'hll_hash_{0}(%s)'.format(self.db_type), (self.get_db_prep_value(connection),)


class HllBoolean(HllPrimitiveValue):
    db_type = 'boolean'

    @classmethod
    def check(cls, data):
        return type(data) is bool

    def get_db_prep_value(self, connection):
        return bool(self._data)


class HllIntegerValue(HllPrimitiveValue):
    # Abstract class property
    value_range = None

    @classmethod
    def check(cls, data):
        return type(data) is int and cls.value_range[0] <= data <= cls.value_range[1]

    def get_db_prep_value(self, connection):
        return int(self._data)


class HllSmallInt(HllIntegerValue):
    db_type = 'smallint'
    value_range = (-32768, 32767)


class HllInteger(HllIntegerValue):
    db_type = 'integer'
    value_range = (-2147483648, 2147483647)


class HllBigint(HllIntegerValue):
    db_type = 'bigint'
    value_range = (-9223372036854775808, 9223372036854775807)


class HllByteA(HllPrimitiveValue):
    db_type = 'bytea'

    @classmethod
    def check(cls, data):
        return isinstance(data, bytes)

    def get_db_prep_value(self, connection):
        return connection.Database.Binary(self._data)


class HllText(HllPrimitiveValue):
    db_type = 'text'

    @classmethod
    def check(cls, data):
        return isinstance(data, six.string_types)

    def get_db_prep_value(self, connection):
        return str(self._data)


class HllAny(HllPrimitiveValue):
    db_type = 'any'

    @classmethod
    def check(cls, data):
        return True


class HllSet(HllDataValue):
    """
    Aggregate of HllValue objects
    """
    def __init__(self, data=None):  # type: (Optional[Any], Optional[int]) -> None
        data = self._parse_iterable(data) if data is not None else set()
        super(HllSet, self).__init__(data)

    @classmethod
    def _parse_item(cls, item):  # type: (Any) -> HllValue
        """
        Parses single item, checking if it can be added to hll and formatting it
        :param item: data to parse
        :return: HllDataValue instances
        """
        if isinstance(item, HllPrimitiveValue):
            pass
        elif isinstance(item, HllValue):
            raise ValueError("Only HllPrimitiveValue instances can be added to HllSet, not %s"
                             % item.__class__.__name__)
        else:
            item = HllDataValue.parse_data(item)

        return item

    @classmethod
    def _parse_iterable(cls, data):  # type: (Iterable[Any]) -> Set[HllValue]
        """
        Parses input iterable into set of HllValue objects
        :param data: Data to parse
        :return: A set of HllValue objects
        """
        return {cls._parse_item(item) for item in data}

    @classmethod
    def check(cls, data):
        return isinstance(data, Iterable)

    def get_sql(self, connection):
        if not self._data:
            return HllEmpty().get_sql(connection)

        tpl = '''
        (SELECT hll_add_agg(val)
        FROM (VALUES %s ) AS values_hll(val))
        '''
        sql, params = zip(*(item.get_sql(connection) for item in self._data))
        values_sql = '(%s)' % '), ('.join(sql)
        return tpl % values_sql, tuple(chain(*params))

    def get_db_prep_value(self, connection):  # type: (DatabaseWrapper) -> Any
        raise NotImplementedError('This method is not used by HllSet class')

    def update(self, other):  # type: (Union[HllSet, Iterable]) -> None
        """
        Adds elements to this HllSet from another iterable
        :return:
        """
        hll_set = other if isinstance(other, HllSet) else self.parse_data(other)
        self._data.update(hll_set._data)

    def add(self, value):  # type: (Any) -> None
        """
        Adds an item to HllSet
        :param value: Value to add
        :return: None
        """
        value = self._parse_item(value)
        self.add(value)

    def __deepcopy__(self, memodict={}):
        return HllSet(deepcopy(self._data))

    def __or__(self, other):
        return deepcopy(self).update(other)


HStoreField