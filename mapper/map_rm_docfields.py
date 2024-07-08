# -*- coding: utf-8 -*-
# @Time    : 2021/8/8 15:28
# @Author  : chenbo


def load(graph, config):
    """
    移除graph中文档对象的字段
    :param graph:
    :param config:
    :return:
    """
    parser_config = config.get("_task").get("parser")
    parser_name = parser_config.get("name", "corenlu")  # 文档解析器名称
    fields = parser_config.get("parse_fields", ["content"])

    docs = filter(lambda node: node.get("metaType") == "document", graph.get("nodes", []))

    for doc in docs:
        for f in fields:
            nlu_f = f"_{parser_name}_{f}"
            if nlu_f in doc:
                del doc[nlu_f]
        for f in config.get("fields", []):
            if f in doc:
                del doc[f]

    return graph
