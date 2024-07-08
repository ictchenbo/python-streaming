# -*- coding: utf-8 -*-
# @Time    : 2021/8/8 15:28
# @Author  : chenbo

from module_manager import manager


def load(doc, config):
    """
    根据定义的转换子对数据进行过滤、转换
    :param doc: 文档
    :param config:
    :return:
    """
    pipes = config.get("pipes", [])
    if not isinstance(pipes, list):  # 支持写单个或数组
        pipes = [pipes]
    for pipe in pipes:
        comp_config = config.get(pipe, {})
        comp_config["_task"] = config["_task"]
        func = manager.load("mapper", f"map_{pipe}")
        if func:
            doc = func(doc, comp_config)
            if doc is None:
                return None  # 实现过滤的效果
    return doc
