# -*- coding: utf-8 -*-
# @Time    : 2021/9/6 21:53
# @Author  : chenbo


def load(doc, config):
    mode = config.get("mode", "all")
    fields = config.get("fields", [])
    if mode == "all":
        # 所有字段不能为空
        for f in fields:
            if f not in doc or not doc[f]:
                return None
        return doc
    elif mode == "any":
        # 至少一个字段不为空
        for f in fields:
            if f in doc and doc[f]:
                return doc
        return None
    elif mode == "none":
        # 所有字段必须为空
        for f in fields:
            if f in doc and doc[f]:
                return None
    return doc
