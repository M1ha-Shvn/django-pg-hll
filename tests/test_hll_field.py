from unittest import skipIf

from django.db import connection
from django.db.models import F
from django.test import TestCase

from django_pg_hll.aggregate import Cardinality, UnionAgg, UnionAggCardinality, CardinalitySum, HllSchemaVersion, \
    HllType, HllLog2M, HllRegWidth, HllExpThreshold, HllSParseOn
from django_pg_hll.compatibility import django_pg_bulk_update_available, string_types
from django_pg_hll.fields import HllField
from django_pg_hll.values import HllEmpty, HllInteger

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

        self.assertIsInstance(instance.hll_field, string_types)
        self.assertEqual(instance.hll_field[:2], r'\x')

        instance.save()

        self.assertEqual(1, TestModel.objects.annotate(card=Cardinality('hll_field')).filter(id=100501).
                         values_list('card', flat=True)[0])

    def test_config_args(self):
        f = HllField(log2m=1, regwidth=2, expthresh=3, sparseon=4)
        self.assertEqual(f.db_type(connection), 'hll(1, 2, 3, 4)')

    def test_partial_config_args(self):
        f = HllField(log2m=1, regwidth=2)
        self.assertEqual(f.db_type(connection), 'hll(1, 2)')

    def test_no_config_args(self):
        f = HllField()
        self.assertEqual(f.db_type(connection), 'hll')

    def test_config_args_wrong_order(self):
        with self.assertRaisesMessage(ValueError, '`regwidth` argument can be set only if [log2m] arguments are set'):
            HllField(regwidth=1)

        with self.assertRaisesMessage(ValueError, '`sparseon` argument can be set only if [log2m, regwidth, expthresh]'
                                                  ' arguments are set'):
            HllField(log2m=1, sparseon=2)

    def test_hll_eq(self):
        self.assertEqual(1, TestModel.objects.filter(hll_field=HllInteger(1)).count())

    def test_hll_ne(self):
        self.assertEqual(2, TestModel.objects.exclude(hll_field=HllInteger(1)).count())


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


class TestConfigurationAggregation(TestCase):
    def setUp(self):
        self.default = TestModel.objects.create(id=100501, hll_field=HllEmpty())
        self.non_default = TestConfiguredModel.objects.create(id=100502, hll_field=HllEmpty(13, 2, 1, 0))

    def test_hll_schema_version(self):
        self.assertEqual(1, TestModel.objects.annotate(ver=HllSchemaVersion('hll_field')).get(id=self.default.id).ver)

    def test_hll_type(self):
        self.assertEqual(1, TestModel.objects.annotate(type=HllType('hll_field')).get(id=self.default.id).type)

    def test_log2m(self):
        self.assertEqual(11, TestModel.objects.annotate(log2m=HllLog2M('hll_field')).get(id=self.default.id).log2m)
        self.assertEqual(13, TestConfiguredModel.objects.annotate(log2m=HllLog2M('hll_field')).
                         get(id=self.non_default.id).log2m)

    def test_regwidth(self):
        self.assertEqual(5, TestModel.objects.annotate(regwidth=HllRegWidth('hll_field')).
                         get(id=self.default.id).regwidth)
        self.assertEqual(2, TestConfiguredModel.objects.annotate(regwidth=HllRegWidth('hll_field')).
                         get(id=self.non_default.id).regwidth)

    def test_expthresh(self):
        self.assertListEqual([-1, 160], TestModel.objects.annotate(expthresh=HllExpThreshold('hll_field')).
                             get(id=self.default.id).expthresh)
        self.assertListEqual([1, 1], TestConfiguredModel.objects.annotate(expthresh=HllExpThreshold('hll_field')).
                             get(id=self.non_default.id).expthresh)

    def test_sparseon(self):
        self.assertEqual(1, TestModel.objects.annotate(sparseon=HllSParseOn('hll_field')).
                         get(id=self.default.id).sparseon)
        self.assertEqual(0, TestConfiguredModel.objects.annotate(sparseon=HllSParseOn('hll_field')).
                         get(id=self.non_default.id).sparseon)

    def test_schema_version_transform_filter(self):
        self.assertEqual(1, TestModel.objects.filter(hll_field__schema_version=1).count())
        self.assertEqual(0, TestModel.objects.filter(hll_field__schema_version=2).count())

    def test_type_transform_filter(self):
        self.assertEqual(1, TestModel.objects.filter(hll_field__type=1).count())
        self.assertEqual(0, TestModel.objects.filter(hll_field__type=2).count())

    def test_log2m_transform_filter(self):
        self.assertEqual(1, TestModel.objects.filter(hll_field__log2m=11).count())
        self.assertEqual(0, TestModel.objects.filter(hll_field__log2m=13).count())
        self.assertEqual(1, TestConfiguredModel.objects.filter(hll_field__log2m=13).count())

    def test_regwidth_transform_filter(self):
        self.assertEqual(1, TestModel.objects.filter(hll_field__regwidth=5).count())
        self.assertEqual(0, TestModel.objects.filter(hll_field__regwidth=2).count())
        self.assertEqual(1, TestConfiguredModel.objects.filter(hll_field__regwidth=2).count())

    def test_sparseon_transform_filter(self):
        self.assertEqual(1, TestModel.objects.filter(hll_field__sparseon=1).count())
        self.assertEqual(0, TestModel.objects.filter(hll_field__sparseon=0).count())
        self.assertEqual(1, TestConfiguredModel.objects.filter(hll_field__sparseon=0).count())


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
