

def django_pg_bulk_update_available() -> bool:
    """
    Tests if django-pb-bulk-update library is installed
    :return: Boolean
    """
    try:
        import django_pg_bulk_update
        return True
    except ImportError:
        return False
