from unittest import skipIf

import six
from django.db import connection
from django.db.models import F
from django.test import TestCase

from django_pg_hll import HllEmpty, HllInteger
from django_pg_hll.aggregate import Cardinality, UnionAgg, UnionAggCardinality, CardinalitySum

# !!! Don't remove this import, or bulk_update will not see function name
from django_pg_hll.bulk_update import HllConcatFunction

from django_pg_hll.compatibility import django_pg_bulk_update_available
from tests.models import TestConfiguredModel, TestModel, FKModel


class HllFieldTest(TestCase):
    def setUp(self):
        self.cursor = connection.cursor()

        TestModel.objects.bulk_create([
            TestModel(id=100501, hll_field=HllEmpty()),
            TestModel(id=100502, hll_field=HllInteger(1)),
            TestModel(id=100503, hll_field=HllInteger(2))
        ])

    def test_combine(self):
        TestModel.objects.create(hll_field=HllEmpty() | HllInteger(1) | HllInteger(2))

    def test_combine_auto_parse(self):
        instance = TestModel.objects.create(hll_field=HllEmpty() | 2 | 3)
        self.assertEqual(2, TestModel.objects.annotate(card=Cardinality('hll_field')).filter(pk=instance.pk).
                         values_list('card', flat=True)[0])

        instance = TestModel.objects.create(hll_field=HllEmpty() | {2, 3})
        self.assertEqual(2, TestModel.objects.annotate(card=Cardinality('hll_field')).filter(pk=instance.pk).
                         values_list('card', flat=True)[0])

    def test_create(self):
        TestModel.objects.create(hll_field=HllEmpty())

    def test_create_custom_params(self):
        TestConfiguredModel.objects.create(hll_field=HllEmpty(13, 2, 1, 0))

    def test_migration(self):
        query = "SELECT hll_cardinality(hll_field) FROM tests_testmodel;"
        self.cursor.execute(query)
        row = self.cursor.fetchone()
        self.assertEqual(0, row[0])

    def test_save(self):
        instance = TestModel.objects.get(id=100501)
        instance.hll_field = HllInteger(1) | F('hll_field')
        instance.save()

        self.assertEqual(1, TestModel.objects.annotate(card=Cardinality('hll_field')).filter(id=100501).
                         values_list('card', flat=True)[0])

        instance.hll_field |= HllInteger(2)
        instance.save()

        self.assertEqual(2, TestModel.objects.annotate(card=Cardinality('hll_field')).filter(id=100501).
                         values_list('card', flat=True)[0])

        # This doesn't work, as I can't change | operator for F() result
        # instance.hll_field = F('hll_field') | HllInteger(3)
        # instance.save()
        #
        # self.assertEqual(3, TestModel.objects.annotate(card=HllCardinality('hll_field')).filter(id=100501).
        #                  values_list('card', flat=True)[0])

    def test_update(self):
        TestModel.objects.filter(id=100501).update(hll_field=HllInteger(1) | F('hll_field'))
        self.assertEqual(1, TestModel.objects.annotate(card=Cardinality('hll_field')).filter(id=100501).
                         values_list('card', flat=True)[0])
        
    def test_hex_convertion(self):
        instance = TestModel.objects.get(id=100501)
        instance.hll_field = HllInteger(1) | F('hll_field')
        instance.save()

        instance.refresh_from_db()

        self.assertIsInstance(instance.hll_field, six.string_types)
        self.assertEqual(instance.hll_field[:2], r'\x')

        instance.save()

        self.assertEqual(1, TestModel.objects.annotate(card=Cardinality('hll_field')).filter(id=100501).
                         values_list('card', flat=True)[0])


class TestAggregation(TestCase):
    def setUp(self):
        TestModel.objects.bulk_create([
            TestModel(id=100501, hll_field=HllEmpty()),
            TestModel(id=100502, hll_field=HllInteger(1) | HllInteger(2)),
            TestModel(id=100503, hll_field=HllInteger(2))
        ])

    def test_cardinality_transform_filter(self):
        self.assertEqual(1, TestModel.objects.filter(hll_field__cardinality=0).count())
        self.assertEqual(1, TestModel.objects.filter(hll_field__cardinality=1).count())
        self.assertEqual(1, TestModel.objects.filter(hll_field__cardinality=2).count())
        self.assertEqual(0, TestModel.objects.filter(hll_field__cardinality=3).count())

    def test_cardinality_aggregate_function(self):
        self.assertEqual({0, 1, 2}, set(TestModel.objects.annotate(card=Cardinality('hll_field')).
                         values_list('card', flat=True)))

    def test_union_aggregate_function(self):
        fk_instance = FKModel.objects.create()
        TestModel.objects.all().update(fk=fk_instance)

        content = FKModel.objects.annotate(union=UnionAgg('testmodel__hll_field')).values_list('union', flat=True)[0]

        # No way to explicitly get cardinality, as django does not support function into function aggregation
        cursor = connection.cursor()
        cursor.execute("SELECT hll_cardinality('%s'::hll);" % content)
        row = cursor.fetchone()
        self.assertEqual(2, row[0])

    def test_union_aggregate_cardinality_function(self):
        fk_instance = FKModel.objects.create()
        TestModel.objects.all().update(fk=fk_instance)

        card = FKModel.objects.annotate(card=UnionAggCardinality('testmodel__hll_field')).\
            values_list('card', flat=True)[0]

        self.assertEqual(2, card)

    def test_cardinality_sum_function(self):
        fk_instance = FKModel.objects.create()
        TestModel.objects.all().update(fk=fk_instance)

        card = FKModel.objects.annotate(card=CardinalitySum('testmodel__hll_field')).\
            values_list('card', flat=True)[0]

        self.assertEqual(3, card)


@skipIf(not django_pg_bulk_update_available(), 'django-pg-bulk-update library is not installed')
class TestBulkUpdate(TestCase):
    def setUp(self):
        TestModel.objects.create(id=100501, hll_field=HllEmpty()),

    def test_bulk_update(self):
        from django_pg_bulk_update import bulk_update

        res = bulk_update(TestModel, [{'id': 100501, 'hll_field': HllInteger(1)}],
                          set_functions={'hll_field': 'hll_concat'})
        self.assertEqual(1, res)

        self.assertEqual(1, TestModel.objects.annotate(card=Cardinality('hll_field')).filter(id=100501).
                         values_list('card', flat=True)[0])

    def test_bulk_update_or_create(self):
        from django_pg_bulk_update import bulk_update_or_create

        res = bulk_update_or_create(TestModel, [{'id': 100501, 'hll_field': HllInteger(1)},
                                                {'id': 100502, 'hll_field': HllInteger(2) | HllInteger(3)}],
                                    set_functions={'hll_field': 'hll_concat'})
        self.assertEqual(2, res)

        self.assertEqual({(100501, 1), (100502, 2)}, set(TestModel.objects.annotate(card=Cardinality('hll_field')).
                                                         values_list('id', 'card')))
