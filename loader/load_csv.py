# -*- coding: utf-8 -*-
# @Time    : 2021/8/8 15:28
# @Author  : chenbo

import csv
from util.time_format import get_time_stamp10


def load(filepath):
    f = open(filepath, 'r', encoding="unicode_escape")
    file_content = csv.reader(f)
    header = []
    for index, rows in enumerate(file_content):
        if index == 0:
            if len(rows) == 1:
                rows = rows[0].split('\t')
            header = rows
        else:
            if len(rows) == 1:
                rows = rows[0].split('\t')
            jsonLine = dict(zip(header, rows))
            if "pt" in jsonLine and jsonLine["pt"] and type(jsonLine["pt"]) is not int:
                jsonLine["pt"] = get_time_stamp10(jsonLine["pt"], "%Y/%m/%d %H:%M")
            yield jsonLine
    f.close()
