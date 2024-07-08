# -*- coding: utf-8 -*-
# @Time    : 2021/8/8 15:28
# @Author  : chenbo

import json
from util.time_format import get_time_stamp10


def load(filepath):
    f = open(filepath, 'r', encoding="utf-8")
    for line in f:
        jsonLine = json.loads(line)
        if "pt" in jsonLine and jsonLine["pt"] and type(jsonLine["pt"]) is not int:
            jsonLine["pt"] = get_time_stamp10(jsonLine["pt"], "%Y-%m-%d %H:%M:%S")
        yield jsonLine
    f.close()
