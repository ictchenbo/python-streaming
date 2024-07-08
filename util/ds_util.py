# -*- coding: utf-8 -*-
# @Time    : 2021/8/8 15:28
# @Author  : chenbo

def map_self(v):
    return v


def map_dict(source: dict or list, mapping: dict):
    if isinstance(source, dict):
        return {mapping[pk]: pv for pk, pv in source.items() if pk in mapping}
    else:
        return [map_dict(item, mapping) for item in source]


def map_reverse(source: dict):
    return {v: k for k, v in source.items()}


def group_by(items, key, default_group=""):
    groups = {}
    for item in items:
        g_key = item.get(key, default_group)
        if g_key in groups:
            groups[g_key].append(item)
        else:
            groups[g_key] = [item]
    return groups
