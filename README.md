# django-pg-hll
Provides a django wrapper for [postgresql-hll library by CitusData](https://github.com/citusdata/postgresql-hll#the-importance-of-hashing)

## Requirements
* Python 2.7 or Python 3.5+
* django >= 1.9
* pytz
* six
* typing
* psycopg2
* PostgreSQL 9.4+   

## Installation
Install via pip:  
`pip install django-pg-hll`    
or via setup.py:  
`python setup.py install`

## Usage
### Prerequisites
Install [postgresql-hll extension](https://github.com/citusdata/postgresql-hll#install)

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
HllField realizes `cardinality` lookup (returning integer value) in order to make filtering easier:
```python
MyModel.objects.filter(hll__cardinality=3).count()
```

### Aggregate functions
In order to count aggregations and annotations, library provides 3 aggregate functions:
* `django_pg_hll.aggregate.Cardinality`
  Counts cardinality of hll field
* `django_pg_hll.aggregate.UnionAgg`
  Aggregates multiple hll fields to one hll.
* `django_pg_hll.aggregate.UnionAggCardinality`
  Counts cardinality of hll, combined by UnionAgg function. In fact, it does `Cardinality(UnionAgg(hll))`.  
  P. s. django doesn't give ability to use function inside function.
```python
from django.db import models
from django_pg_hll import HllField, HllInteger
from django_pg_hll.aggregate import Cardinality, UnionAggCardinality


class ForeignModel(models.Model):
    pass
  
  
class MyModel(models.Model):
    hll = HllField()
    fk = models.ForeignKey(ForeignModel)
    
MyModel.objects.bulk_create([
   MyModel(fk=1, hll=HllInteger(1)),
   MyModel(fk=2, hll=HllInteger(2) | HllInteger(3)),
   MyModel(fk=3, hll=HllInteger(4))
])

MyModel.objects.annotate(card=Cardinality('hll_field')).values_list('id', 'card')
# outputs (1, 1), (2, 2), (3, 1)

# Count cardinality for hll, built from 
ForeignModel.objects.annotate(card=UnionAggCardinality('testmodel__hll_field')).values_list('card', flat=True)
# outputs [4]
```
 
### [django-pg-bulk-update](https://github.com/M1hacka/django-pg-bulk-update) integration
This library provides a `hll_concat` set function,
allowing to use hll in `bulk_update` and `bulk_update_or_create` queries.
```python
# !!! Don't forget to import function, or django_pg_bulk_update will not find it
from django_pg_hll.bulk_update import HllConcatFunction

MyModel.objects.bulk_update_or_create([
    {'id': 100501, 'hll_field': HllInteger(1)},
    {'id': 100502, 'hll_field': HllInteger(2) | HllInteger(3)}
    ], set_functions={'hll_field': 'hll_concat'}
)
```
