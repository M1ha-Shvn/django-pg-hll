from typing import Union

import django


def psycopg_binary_to_bytes(data):  # type: (Union[bytes, 'Binary']) -> bytes
    """
    Since django 3.2 psycopg2.extensions.Binary is returned for BinaryField instead of raw byte string
    :param data: Data to convert to bytes
    :return:
    """
    if django.VERSION < (3, 2):
        return data

    from psycopg2.extensions import Binary
    if not isinstance(data, Binary):
        return data

    return data.adapted
