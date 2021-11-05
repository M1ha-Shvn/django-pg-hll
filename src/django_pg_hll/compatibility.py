import sys


def django_pg_bulk_update_available():  # type: () -> bool
    """
    Tests if django-pb-bulk-update library is installed
    :return: Boolean
    """
    try:
        import django_pg_bulk_update  # noqa: F401
        return True
    except ImportError:
        return False


try:
    # This approach applies to python 3.10+
    from collections.abc import Iterable  # noqa F401
except ImportError:
    # This approach applies to python versions less than 3.10
    from collections import Iterable  # noqa F401


# six.string_types replacement in order to remove dependency
string_types = (str,) if sys.version_info[0] == 3 else (str, unicode)  # noqa F821
