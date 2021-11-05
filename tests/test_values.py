from unittest import TestCase

from django.db import connection
from django.db.models.sql import Query

from django_pg_hll import HllEmpty, HllSmallInt, HllInteger, HllBigint, HllBoolean, HllByteA, HllText, HllAny, HllSet
from tests.compatibility import psycopg_binary_to_bytes
from tests.models import TestModel


class ValueTest(TestCase):
    def setUp(self):
        self.compiler = Query(TestModel).get_compiler(connection=connection)


class HllEmptyTest(ValueTest):
    def test_sql(self):
        val = HllEmpty()
        sql, params = val.as_sql(self.compiler, connection)
        self.assertEqual('hll_empty()', sql)
        self.assertListEqual([], params)

    def test_sql_with_args(self):
        val = HllEmpty(1, 2, 3, 4)
        sql, params = val.as_sql(self.compiler, connection)
        self.assertEqual('hll_empty(%s, %s, %s, %s)', sql)
        self.assertListEqual([1, 2, 3, 4], params)

    def test_sql_with_partial_args(self):
        val = HllEmpty(1, 2)
        sql, params = val.as_sql(self.compiler, connection)
        self.assertEqual('hll_empty(%s, %s)', sql)
        self.assertListEqual([1, 2], params)


class HllSmallIntTest(ValueTest):
    def test_sql(self):
        val = HllSmallInt(1)
        sql, params = val.as_sql(self.compiler, connection)
        self.assertEqual('hll_empty() || hll_hash_smallint(%s::smallint)', sql)
        self.assertListEqual([1], params)

    def test_check(self):
        # Check correct values
        HllSmallInt(32767)
        HllSmallInt(-32768)
        HllSmallInt(0)

        with self.assertRaises(TypeError):
            HllSmallInt()

        with self.assertRaises(ValueError):
            HllSmallInt(32768)

        with self.assertRaises(ValueError):
            HllSmallInt(-32769)

        with self.assertRaises(ValueError):
            HllSmallInt('test')

        with self.assertRaises(ValueError):
            HllSmallInt(True)


class HllIntegerTest(ValueTest):
    def test_sql(self):
        val = HllInteger(1)
        sql, params = val.as_sql(self.compiler, connection)
        self.assertEqual('hll_empty() || hll_hash_integer(%s::integer)', sql)
        self.assertListEqual([1], params)

    def test_check(self):
        # Check correct values
        HllInteger(2147483647)
        HllInteger(-2147483648)
        HllInteger(0)

        with self.assertRaises(TypeError):
            HllInteger()

        with self.assertRaises(ValueError):
            HllInteger(2147483648)

        with self.assertRaises(ValueError):
            HllInteger(-2147483649)

        with self.assertRaises(ValueError):
            HllInteger('test')

        with self.assertRaises(ValueError):
            HllInteger(True)


class HllBigintTest(ValueTest):
    def test_sql(self):
        val = HllBigint(1)
        sql, params = val.as_sql(self.compiler, connection)
        self.assertEqual('hll_empty() || hll_hash_bigint(%s::bigint)', sql)
        self.assertListEqual([1], params)

    def test_check(self):
        # Check correct values
        HllBigint(9223372036854775807)
        HllBigint(-9223372036854775808)
        HllBigint(0)

        with self.assertRaises(TypeError):
            HllBigint()

        with self.assertRaises(ValueError):
            HllBigint(9223372036854775808)

        with self.assertRaises(ValueError):
            HllBigint(-9223372036854775809)

        with self.assertRaises(ValueError):
            HllBigint('test')

        with self.assertRaises(ValueError):
            HllBigint(True)


class HllBooleanTest(ValueTest):
    def test_sql(self):
        val = HllBoolean(True)
        sql, params = val.as_sql(self.compiler, connection)
        self.assertEqual('hll_empty() || hll_hash_boolean(%s)', sql)
        self.assertListEqual([True], params)

    def test_check(self):
        # Check correct values
        HllBoolean(True)
        HllBoolean(False)

        with self.assertRaises(TypeError):
            HllBoolean()

        with self.assertRaises(ValueError):
            HllBoolean(1)

        with self.assertRaises(ValueError):
            HllBoolean(None)

        with self.assertRaises(ValueError):
            HllBoolean('test')


class HllByteATest(ValueTest):
    def test_sql(self):
        val = HllByteA(b'abc')
        sql, params = val.as_sql(self.compiler, connection)
        self.assertEqual('hll_empty() || hll_hash_bytea(%s)', sql)
        self.assertListEqual([b'abc'], [psycopg_binary_to_bytes(param) for param in params])

    def test_check(self):
        # Check correct values
        HllByteA(b'abc')
        HllByteA(b'')

        with self.assertRaises(TypeError):
            HllByteA()

        with self.assertRaises(ValueError):
            HllByteA(1)

        with self.assertRaises(ValueError):
            HllByteA(None)


class HllTextTest(ValueTest):
    def test_sql(self):
        val = HllText('test')
        sql, params = val.as_sql(self.compiler, connection)
        self.assertEqual('hll_empty() || hll_hash_text(%s)', sql)
        self.assertListEqual(['test'], params)

    def test_check(self):
        # Check correct values
        HllText('test')
        HllText('')

        with self.assertRaises(TypeError):
            HllText()

        with self.assertRaises(ValueError):
            HllText(1)

        with self.assertRaises(ValueError):
            HllText(None)

        with self.assertRaises(ValueError):
            HllText(True)


class HllAnyTest(ValueTest):
    def test_sql(self):
        val = HllAny(True)
        sql, params = val.as_sql(self.compiler, connection)
        self.assertEqual('hll_empty() || hll_hash_any(%s)', sql)
        self.assertListEqual([True], params)

    def test_check(self):
        # Check correct values
        HllAny(True)
        HllAny(1)
        HllAny(100500)
        HllAny('test')
        HllAny(b'test')

        with self.assertRaises(TypeError):
            HllBoolean()


class HllSetTest(ValueTest):
    def test_sql(self):
        val = HllSet([1])
        sql, params = val.as_sql(self.compiler, connection)
        self.assertEqual('hll_empty() || hll_hash_smallint(%s::smallint)', sql)
        self.assertListEqual([1], params)

        val = HllSet([1, 100500])
        sql, params = val.as_sql(self.compiler, connection)
        self.assertEqual('hll_empty() || hll_hash_smallint(%s::smallint) || hll_hash_integer(%s::integer)', sql)
        self.assertListEqual([1, 100500], params)

        val = HllSet()
        sql, params = val.as_sql(self.compiler, connection)
        self.assertEqual('hll_empty()', sql)
        self.assertListEqual([], params)

        val = HllSet(HllInteger(1))
        sql, params = val.as_sql(self.compiler, connection)
        self.assertEqual('hll_empty() || hll_hash_integer(%s::integer)', sql)
        self.assertListEqual([1], params)

    def test_check(self):
        # Check correct values
        HllSet()
        HllSet([False, 1, 4, 'test'])
        HllSet('test')  # String is iterable

        with self.assertRaises(ValueError):
            HllSet(1)

        with self.assertRaises(ValueError):
            HllSet(None)

        with self.assertRaises(ValueError):
            HllSet(True)
