# -*- coding: utf-8 -*-
# @Time    : 2021/9/6 21:53
# @Author  : chenbo


def load(doc, config):
    """
        根据内容进行过滤
        :param doc:
        :param config:
        :return:
        """
    c_fields = config.get("fields", ["cont", "content"])
    cont = None
    for f in c_fields:
        if f in doc:
            cont = doc.pop(f)
    if cont is None:
        return None
    doc["content"] = cont

    return doc
