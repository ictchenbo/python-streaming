# -*- coding: utf-8 -*-
# @Time    : 2021/9/6 21:53
# @Author  : chenbo


def load(doc, config):
    if "fields" in config:
        select = config.get("fields", [])
        if "_id" not in select:
            select.append("_id")
        ret = {key: doc.get(key) for key in select if key in doc}
        return ret
    return doc
