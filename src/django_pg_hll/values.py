from collections import Iterable
from typing import Any, Set

import six
from abc import abstractmethod, ABC
from django.db.models import Func
from django.db.models.expressions import CombinedExpression, F

from django_pg_hll.utils import get_subclasses


class HllJoinMixin:
    CONCAT = '||'

    def __or__(self, other):
        # Functions, field references and other HllValues shouldn't be parsed
        if isinstance(other, (F, HllValue, Func)):
            val = other
        else:
            val = HllDataValue.parse_data(other)
        return HllCombinedExpression(self, self.CONCAT, val)


class HllCombinedExpression(HllJoinMixin, CombinedExpression):
    pass


class HllValue(ABC, HllJoinMixin, Func):
    pass


class HllEmpty(HllValue):
    function = 'hll_empty'

    def __init__(self, **extra):
        super(HllEmpty, self).__init__(**extra)


class HllDataValue(HllValue):
    def __init__(self, data, *args, **extra):
        if not self.check(data):
            raise ValueError('Data is not supported by %s' % self.__class__.__name__)

        super(HllDataValue, self).__init__(data, *args, **extra)

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


class HllPrimitiveValue(HllDataValue, ABC):
    # Abstract class property
    db_type = None
    template = 'hll_empty() || %(function)s(%(expressions)s)'

    def __init__(self, data, **extra):  # type: (Any, **dict) -> None
        """
        :param data: Data to build value from
        :param hash_seed: Optional hash seed. See https://github.com/citusdata/postgresql-hll#the-importance-of-hashing
        """
        hash_seed = extra.pop('hash_seed', None)
        if hash_seed is not None:
            super(HllPrimitiveValue, self).__init__(data, hash_seed, **extra)
        else:
            super(HllPrimitiveValue, self).__init__(data, **extra)

    @property
    def function(self):
        return 'hll_hash_%s' % self.db_type


class HllBoolean(HllPrimitiveValue):
    db_type = 'boolean'

    @classmethod
    def check(cls, data):
        return type(data) is bool


class HllIntegerValue(HllPrimitiveValue):
    # Abstract class property
    value_range = None

    @classmethod
    def check(cls, data):
        return type(data) is int and cls.value_range[0] <= data <= cls.value_range[1]


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


class HllText(HllPrimitiveValue):
    db_type = 'text'

    @classmethod
    def check(cls, data):
        return isinstance(data, six.string_types)


class HllAny(HllPrimitiveValue):
    db_type = 'any'

    @classmethod
    def check(cls, data):
        return True


class HllSet(HllDataValue):
    """
    Aggregate of HllValue objects
    """
    def __init__(self, *args, **extra):
        if args:
            data = self._parse_iterable(args[0])
            args = args[:1]
        else:
            data = set()
        super(HllSet, self).__init__(data, *args, **extra)

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
        res = HllEmpty()
        for item in data:
            res |= cls._parse_item(item)
        return res

    @classmethod
    def check(cls, data):
        return isinstance(data, Iterable)

    # def update(self, other):  # type: (Union[HllSet, Iterable]) -> None
    #     """
    #     Adds elements to this HllSet from another iterable
    #     :return:
    #     """
    #     hll_set = other if isinstance(other, HllSet) else self._parse_iterable(other)
    #     self
    #
    # def add(self, value):  # type: (Any) -> None
    #     """
    #     Adds an item to HllSet
    #     :param value: Value to add
    #     :return: None
    #     """
    #     value = self._parse_item(value)
    #     self.add(value)
