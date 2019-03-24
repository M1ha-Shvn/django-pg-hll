from collections import Iterable
from copy import deepcopy
from typing import Any, Set

import six
from abc import abstractmethod, ABCMeta
from django.db.models import Func
from django.db.models.expressions import CombinedExpression, F

from django_pg_hll.utils import get_subclasses


class HllJoinMixin:
    CONCAT = '||'

    def __or__(self, other):
        # Functions, field references and other HllValues shouldn't be parsed
        if not isinstance(other, (F, HllValue, Func)):
            other = HllDataValue.parse_data(other)
        else:
            other = deepcopy(other)

        if isinstance(other, HllDataValue):
            other.added_to_hll_set()

        return HllCombinedExpression(self, self.CONCAT, other)


class HllCombinedExpression(HllJoinMixin, CombinedExpression):
    pass


class HllValue(six.with_metaclass(ABCMeta, HllJoinMixin, Func)):
    pass


class HllEmpty(HllValue):
    function = 'hll_empty'

    def __init__(self, **extra):
        super(HllEmpty, self).__init__(**extra)


class HllDataValue(HllValue):
    # This value is used to form real template and can be redeclared in descendants
    base_template = '%(function)s(%(expressions)s)'

    def __init__(self, data, *args, **extra):
        if not self.check(data):
            raise ValueError('Data is not supported by %s' % self.__class__.__name__)

        # hll_empty() is added in order to init set, if it's create or bulk operation
        if 'template' not in extra:
            extra['template'] = 'hll_empty() || %s' % self.base_template

        super(HllDataValue, self).__init__(data, *args, **extra)

    def added_to_hll_set(self):
        # Remove hll_empty() prefix from value, it will be added by set
        self.extra['template'] = self.base_template

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
        # !!! Class order is important here!!! Can't use get_subclasses
        for klass in (HllBoolean, HllSmallInt, HllInteger, HllBigint, HllByteA, HllText, HllSet, HllAny):
            if klass.check(data):
                return klass(data)

        raise ValueError('No appropriate class found for value of type: %s' % str(type(data)))


class HllPrimitiveValue(six.with_metaclass(ABCMeta, HllDataValue)):
    # Abstract class property
    db_type = None

    def __init__(self, data, **extra):  # type: (Any, **dict) -> None
        """
        :param data: Data to build value from
        :param hash_seed: Optional hash seed. See https://github.com/citusdata/postgresql-hll#the-importance-of-hashing
        """
        extra['db_type'] = self.db_type

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
    base_template = '%(function)s(%(expressions)s::%(db_type)s)'

    @classmethod
    def check(cls, data):
        return cls.value_range and type(data) is int and cls.value_range[0] <= data <= cls.value_range[1]


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
    base_template = '%(expressions)s'

    def __init__(self, *args, **extra):
        if args:
            data = self._parse_iterable(args[0])
            args = args[1:]
        else:
            data = HllEmpty()

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
    def _parse_iterable(cls, data):  # type: (Iterable[Any]) -> HllCombinedExpression
        """
        Parses input iterable into set of HllValue objects
        :param data: Data to parse
        :return: A set of HllValue objects
        """
        it = iter(data)
        res = cls._parse_item(next(it))

        for item in it:
            res |= cls._parse_item(item)

        return res

    @classmethod
    def check(cls, data):
        return isinstance(data, (Iterable, HllCombinedExpression))
