def slice_dict(dictionary: dict, keys: list):
    return {k: v for k, v in dictionary if k in keys}
