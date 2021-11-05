from django.db import models, migrations

from django_pg_hll import HllField
from django_pg_hll.migration import HllExtension


class Migration(migrations.Migration):
    initial = True
    dependencies = []

    operations = [
        HllExtension(),
        migrations.CreateModel(
            name='FkModel',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID'))
            ],
            options={
                'abstract': False,
            }
        ),
        migrations.CreateModel(
            name='TestModel',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('hll_field', HllField()),
                ('fk', models.ForeignKey('FKModel', null=True, blank=True, on_delete=models.CASCADE))
            ],
            options={
                'abstract': False,
            }
        ),
        migrations.CreateModel(
            name='TestConfiguredModel',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('hll_field', HllField(log2m=13, regwidth=2, expthresh=1, sparseon=0)),
            ],
            options={
                'abstract': False,
            }
        )
    ]
