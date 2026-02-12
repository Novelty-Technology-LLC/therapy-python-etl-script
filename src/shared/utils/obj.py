def get_obj_value(data, *keys, default=None):
    """
    Optional chaining for nested dictionaries.
    Similar to JavaScript's obj?.key1?.key2

    Usage:
        optional_chain(obj, 'address', 'city')
        optional_chain(obj, 'address', 'country', default='Unknown')
    """
    current = data
    for key in keys:
        if isinstance(current, dict):
            current = current.get(key)
            if current is None:
                return default
        else:
            return default

    return current if current is not None else default
