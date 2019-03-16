from django.db import connection
from django.test import TestCase

from django_pg_hll import HllEmpty, HllInteger
from django_pg_hll.aggregate import HllCardinality, HllUnionAgg
from tests.models import TestModel, FKModel


class ModelTest(TestCase):
    def setUp(self):
        self.cursor = connection.cursor()
        TestModel.objects.bulk_create([
            TestModel(hll_field=HllEmpty()),
            TestModel(hll_field=HllInteger(1)),
            TestModel(hll_field=HllInteger(2))
        ])

    def test_combine(self):
        TestModel.objects.create(hll_field=HllEmpty() | HllInteger(1) | HllInteger(2))

    def test_create(self):
        TestModel.objects.create(hll_field=HllEmpty())

    def test_migration(self):
        query = "SELECT hll_cardinality(hll_field) FROM tests_testmodel;"
        self.cursor.execute(query)
        row = self.cursor.fetchone()
        self.assertEqual(0, row[0])

    def test_cardinality_transform_filter(self):
        self.assertEqual(1, TestModel.objects.filter(hll_field__cardinality=0).count())
        self.assertEqual(2, TestModel.objects.filter(hll_field__cardinality=1).count())
        self.assertEqual(0, TestModel.objects.filter(hll_field__cardinality=2).count())

    def test_cardinality_aggregate_function(self):
        self.assertEqual({0, 1}, set(TestModel.objects.annotate(card=HllCardinality('hll_field')).
                         values_list('card', flat=True)))

    def test_union_aggregate_function(self):
        fk_instance = FKModel.objects.create()
        TestModel.objects.all().update(fk=fk_instance)

        content = FKModel.objects.annotate(union=HllUnionAgg('testmodel__hll_field')).values_list('union', flat=True)[0]

        # No way to explicitly get cardinality...
        t = TestModel.objects.create(hll_field=content)
        self.assertEqual(2, set(TestModel.objects.filter(id=t.id).annotate(card=HllCardinality('hll_field')).
                                values_list('card', flat=True)))
