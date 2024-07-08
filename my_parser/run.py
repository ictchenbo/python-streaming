# -*- coding: utf-8 -*-
# @Time    : 2021/8/8 15:28
# @Author  : chenbo

from module_manager import manager


def load(doc, parse_config):
    """
    对一批文档进行解析
    :param doc: 一篇文档(dict)或多篇文档(list of dict)
    :param parse_config: 解析配置
    :return:
    """
    parser_name = parse_config.get("name", "corenlu")
    parse = manager.load("my_parser", f"nlu_parse_{parser_name}")
    if parse is None:
        print(f"my_parser {parser_name} not defined")
        return doc
    print("parser", parser_name)
    parse(doc, parse_config)
    return doc
