[![Python unit tests](https://github.com/M1ha-Shvn/django-pg-hll/actions/workflows/python-tests.yml/badge.svg)](https://github.com/M1ha-Shvn/django-pg-hll/actions/workflows/python-tests.yml)  [![Upload Python Package](https://github.com/M1ha-Shvn/django-pg-hll/actions/workflows/python-publish.yml/badge.svg)](https://github.com/M1ha-Shvn/django-pg-hll/actions/workflows/python-publish.yml) [![Downloads](https://pepy.tech/badge/django-pg-hll/month)](https://pepy.tech/project/django-pg-hll)


# django-pg-hll
Provides a django wrapper for [postgresql-hll library by CitusData](https://github.com/citusdata/postgresql-hll#the-importance-of-hashing)

## Requirements
* Python 3.5+  
* django >= 1.9 (tested 2.2+)  
* PostgreSQL 9.4+ (tested 9.6+)  

## Installation
Install via pip:  
`pip install django-pg-hll`    
or via setup.py:  
`python setup.py install`

## Usage
### Prerequisites
Install [postgresql-hll extension](https://github.com/citusdata/postgresql-hll#install)

#### Creating hll extension
If your user has super-admin privileges you can create Hll extension using migrations.
If you use django 1.10+ you can use `django_pg_hll.migrations.HllExtension` in your migration file.
If you have older version you can use the following:
```python
migrations.RunSQL('CREATE EXTENSION IF NOT EXISTS hll;', reverse_sql='DROP EXTENSION hll;')
```

### Creating table with hll field
* Add HllField to your model:
  ```python
  from django.db import models
  from django_pg_hll import HllField
  
  class MyModel(models.Model):
      hll = HllField()
  ```
* Call [makemigrations](https://docs.djangoproject.com/en/2.1/ref/django-admin/#django-admin-makemigrations) to create a migration
* Call [migrate](https://docs.djangoproject.com/en/2.1/ref/django-admin/#django-admin-migrate) to apply migration.

### Hll values
In order to create and update Hll this library introduces a set of functions 
(corresponding to [postgres-hll hash functions](https://github.com/citusdata/postgresql-hll#hashing)),
 to hash values:
```python
from django_pg_hll import HllField

# Empty hll
HllEmpty()

# Empty hll with custom configuration parameters
# hll_empty([log2m[, regwidth[, expthresh[, sparseon]]]])
HllEmpty(13, 2, 1, 0)

# Hash from boolean
HllBoolean(True)

# Hash from integer with different ranges
HllSmallInt(1)
HllInteger(65540)
HllBigint(2147483650)

# Hash from bytes sequence
HllByteA(b'test')

# Hash from text
HllText('test')

# Auto detection of type by postgres-hll
HllAny('some data')
```

To save a value to HllField, you can pass any of these functions as a value:
```python
from django_pg_hll import HllInteger

instance = MyModel.objects.create(hll=HllInteger(123))
instance.hll |= HllInteger(456)
instance.save()
```

#### Chaining hll values
Hll values can be chained with each other and functions like `django.db.models.F` using `|` operator.  
The chaining result will be `django_pg_hll.values.HllSet` instance, which can be also saved to database.  
You can also chain simple values and iterables. 
In this case, library will try to detect appropriate hashing function, based on value.  
*Important*: Native django functions can't be used as chain start, as `|` operator is redeclared for HllValue instances.  
Example:
```python
from django_pg_hll import HllInteger
from django.db.models import F

instance = MyModel.objects.create(hll=HllInteger(123))

# This works
instance.hll |= HllInteger(456)
instance.hll = HllInteger(456) | F('hll')
instance.hll |= 789  # HllSmallInt will be used
instance.hll |= 100500  # HllInteger will be used
instance.hll |= True  # HllBoolean will be used
instance.hll |= {1, 2, 3, 4, 5}  # set. HllSmallInt will be used.

# This throws exception, as F function doesn't support bitor operator
instance.hll = F('hll') | HllInteger(456)
```
 
#### Hashing seed
You can pass `hash_seed` optional argument to any HllValue, expecting data.  
[Look here](https://github.com/citusdata/postgresql-hll#the-importance-of-hashing) for more details about hashing.


### Filtering QuerySet
HllField realizes several lookups (returning float value) in order to make filtering easier:
```python
# Equality
MyModel.objects.filter(hll=HllInteger(1)).count()
MyModel.objects.exclude(hll=HllInteger(2)).count()

# Cardinality
MyModel.objects.filter(hll__cardinality=3).count()

# Configuration lookups
MyModel.objects.filter(hll__schema_version=1).count()
MyModel.objects.filter(hll__type=1).count()
MyModel.objects.filter(hll__log2m=11).count()
MyModel.objects.filter(hll__regwidth=2).count()
MyModel.objects.filter(hll__sparseon=1).count()
```

### Aggregate functions
In order to count aggregations and annotations, library provides aggregate functions:
* `django_pg_hll.aggregate.Cardinality`
  Counts cardinality of hll field
* `django_pg_hll.aggregate.UnionAgg`
  Aggregates multiple hll fields to one hll.
* `django_pg_hll.aggregate.UnionAggCardinality`
  Counts cardinality of hll, combined by UnionAgg function. In fact, it does `Cardinality(UnionAgg(hll))`.  
  P. s. django doesn't give ability to use function inside function.
* `django_pg_hll.aggregate.CardinalitySum`
  Counts sum of multiple rows hll cardinalities. In fact, it does `Sum(Cardinality(hll))`.  
  P. s. django doesn't give ability to use function inside function.
```python
from django.db import models
from django_pg_hll.aggregate import Cardinality, UnionAggCardinality, CardinalitySum
from django_pg_hll.fields import HllField
from django_pg_hll.values import HllInteger


class ForeignModel(models.Model):
    pass
  
  
class MyModel(models.Model):
    hll = HllField()
    fk = models.ForeignKey(ForeignModel)
    
MyModel.objects.bulk_create([
   MyModel(fk=1, hll=HllInteger(1)),
   MyModel(fk=2, hll=HllInteger(2) | HllInteger(3) | HllInteger(4)),
   MyModel(fk=3, hll=HllInteger(4))
])

MyModel.objects.annotate(card=Cardinality('hll_field')).values_list('id', 'card')
# outputs (1, 1), (2, 3), (3, 1)

# Count cardinality for hll, build by union of all rows
# 4 element exists in rows with fk=2 and fk=3. After union it gives single result 
ForeignModel.objects.annotate(card=UnionAggCardinality('testmodel__hll_field')).values_list('card', flat=True)
# outputs [4]

# Count sum of cardinalities for each row
ForeignModel.objects.annotate(card=CardinalitySum('testmodel__hll_field')).values_list('card', flat=True)
# outputs [5]
```


### Configuration aggregate functions
In order to get hll field creation parameters, library provides aggregate functions:
* `django_pg_hll.aggregate.HllSchemaVersion`
  Returns the schema version value (integer) of the hll  
  
* `django_pg_hll.aggregate.HllType`
  Returns the schema version-specific type value (integer) of the hll. 
  See the [storage specification (v1.0.0)](https://github.com/aggregateknowledge/hll-storage-spec/blob/v1.0.0/STORAGE.md) 
   for more details.
   
* `django_pg_hll.aggregate.HllRegWidth`
  Returns the register bit-width (integer) of the hll  
  
* `django_pg_hll.aggregate.HllLog2M`
  Returns the log-base-2 of the number of registers of the hll. 
  If the hll is not of type FULL or SPARSE it returns the log2m value which would be used if the hll were promoted.
  
* `django_pg_hll.aggregate.HllExpThreshold`
  Returns an array with 2 elements of the specified and effective EXPLICIT promotion cutoffs for the hll.
  The specified cutoff and the effective cutoff will be the same unless expthresh has been set to 'auto' (-1).
  In that case the specified value will be -1 and the effective value will be the implementation-dependent number 
   of explicit values that will be stored before an EXPLICIT hll is promoted.
  
* `django_pg_hll.aggregate.HllSParseOn`
  Returns 1 if the SPARSE representation is enabled for the hll, and 0 otherwise  
 
```python
from django.db import models
from django_pg_hll.aggregate import HllLog2M
from django_pg_hll.fields import HllField
from django_pg_hll.values import HllEmpty, HllInteger


class MyModel(models.Model):
    default_hll = HllField()
    configured_hll = HllField(log2m=13, regwidth=2, expthresh=1, sparseon=0)
    
MyModel.objects.create(fk=1, hll=HllInteger(1), configured_hll=HllEmpty(13, 2, 1, 0))

MyModel.objects.annotate(log2m=HllLog2M('default_hll'), log2m_conf=HllLog2M('configured_hll')). \
    values_list('log2m', 'log2m_conf')
# outputs (11, 13)
```

 
### [django-pg-bulk-update](https://github.com/M1hacka/django-pg-bulk-update) integration
This library provides a `hll_concat` set function,
allowing to use hll in `bulk_update` and `bulk_update_or_create` queries.
```python
MyModel.objects.bulk_update_or_create([
    {'id': 100501, 'hll_field': HllInteger(1)},
    {'id': 100502, 'hll_field': HllInteger(2) | HllInteger(3)}
    ], set_functions={'hll_field': 'hll_concat'}
)
```


## Running tests
### Running in docker
1. Install [docker and docker-compose](https://www.docker.com/)
2. Run `docker build . --tag django-pg-hll` in project directory
3. Run `docker-compose run run_tests` in project directory  

### Running in virtual environment
1. Install all requirements listed above  
2. [Create virtual environment](https://docs.python.org/3/tutorial/venv.html)  
3. Create a superuser named 'test' on your local Postgres instance:
  ```sql
  CREATE ROLE test;
  ALTER ROLE test WITH SUPERUSER;
  ALTER ROLE test WITH LOGIN;
  ALTER ROLE test PASSWORD 'test';
  CREATE DATABASE test OWNER test;
  ```   
3. Install requirements   
  `pip3 install -U -r requirements-test.txt`  
4. Start tests  
  `python3 runtests.py`  
   