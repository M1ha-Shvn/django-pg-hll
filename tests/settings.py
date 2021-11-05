"""
This file contains django settings to run tests with runtests.py
"""
import os

SECRET_KEY = 'fake-key'

DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.postgresql_psycopg2',
        'NAME': 'test',
        'USER': os.environ.get('PGUSER', 'test'),
        'PASSWORD': os.environ.get('PGPASS', 'test'),
        'HOST': os.environ.get('PGHOST', '127.0.0.1'),
        'PORT': '5432'
    }
}

INSTALLED_APPS = [
    "src",
    "tests"
]
