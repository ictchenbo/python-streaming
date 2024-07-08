# -*- coding: utf-8 -*-
# @Time    : 2021/8/8 15:28
# @Author  : chenbo

def load(data, config=None):
    """
    将数据打印到控制台
    :param data:
    :param config:
    :return:
    """
    if isinstance(data, list):
        for item in data:
            print(item)
    else:
        print(data)
    return True
