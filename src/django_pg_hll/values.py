from copy import deepcopy
from typing import Any

from abc import abstractmethod, ABCMeta
from django.db.models.expressions import CombinedExpression, F, Func, Value

from .compatibility import string_types, Iterable


class HllJoinMixin:
    CONCAT = '||'

    def __or__(self, other):  # type: (Any) -> HllCombinedExpression
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


class HllFromHex(Func, metaclass=ABCMeta):
    """
    Constructs hll that can be saved from binary data (or it's psycopg representation)
    """
    def __init__(self, data, *args, **extra):
        db_type = extra.pop('db_type', 'hll')

        # Psycopg2 returns Binary results as hex string, prefixed by \x but requires bytes for saving.
        if isinstance(data, string_types) and data.startswith(r'\x'):
            data = bytearray.fromhex(data[2:])
        elif isinstance(data, bytes):
            pass
        else:
            raise ValueError('data should be bytes instance or string starting with \\x')

        self.template = extra.get('template', '%(expressions)s::{}'.format(db_type))

        super(HllFromHex, self).__init__(Value(data), *args, **extra)


class HllValue(HllJoinMixin, Func, metaclass=ABCMeta):
    pass


class HllEmpty(HllValue):
    function = 'hll_empty'


class HllDataValue(HllValue):
    # This value is used to form real template and can be redeclared in descendants
    base_template = '%(function)s(%(expressions)s)'

    def __init__(self, data, *args, **extra):
        if not self.check(data):
            raise ValueError('Data is not supported by %s' % self.__class__.__name__)

        # hll_empty() is added in order to init set, if it's create or bulk operation
        if 'template' not in extra:
            extra['template'] = 'hll_empty() || %s' % self.base_template

        super(HllDataValue, self).__init__(Value(data), *args, **extra)

    def added_to_hll_set(self):  # type: () -> None
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


class HllPrimitiveValue(HllDataValue, metaclass=ABCMeta):
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
        return isinstance(data, string_types)


class HllAny(HllPrimitiveValue):
    db_type = 'any'

    @classmethod
    def check(cls, data):
        return True


class HllSet(HllValue):
    """
    Aggregate of HllValue objects
    """
    def __init__(self, *args, **extra):
        if args:
            if not self.check(args[0]):
                raise ValueError('Data is not supported by %s' % self.__class__.__name__)

            if isinstance(args[0], HllValue):
                self.data = (args[0],)

                if isinstance(args[0], HllDataValue):
                    args[0].added_to_hll_set()
            else:
                self.data = tuple(self._parse_item(item) for item in args[0])

            args = args[1:]
        else:
            self.data = tuple()

        super(HllSet, self).__init__(*args, **extra)

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

        item.added_to_hll_set()

        return item

    @classmethod
    def check(cls, data):
        return isinstance(data, (Iterable, HllValue))

    def as_sql(self, compiler, connection, function=None, template=None):
        sql, params = HllEmpty().as_sql(compiler, connection)

        for item in self.data:
            item_sql, item_params = item.as_sql(compiler, connection)
            sql += ' || %s' % item_sql
            params.extend(item_params)

        return sql, params
