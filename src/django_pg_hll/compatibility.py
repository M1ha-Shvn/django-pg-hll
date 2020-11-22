

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
