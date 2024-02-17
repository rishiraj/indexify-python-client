from enum import Enum


def json_set_default(obj):
    if isinstance(obj, set):
        return list(obj)
    raise TypeError
