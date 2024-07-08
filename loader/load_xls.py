# -*- coding: utf-8 -*-
# @Time    : 2021/8/8 15:28
# @Author  : chenbo

import pandas
from util.time_format import get_time_stamp10


def load(filepath):
    xl = pandas.ExcelFile(filepath)

    for sheetIndex in range(0, len(xl.sheet_names)):
        row_list = []
        sheet_name = xl.sheet_names[sheetIndex]
        current_sheet = pandas.read_excel(filepath, sheet_name=str(sheet_name), header=None)

        # 获取最大行和最大列数
        nrows = current_sheet.shape[0]
        ncols = current_sheet.columns.size

        if nrows <= 0 or ncols <= 0:
            continue

        for iRow in range(nrows):
            temp = []
            for iCol in range(ncols):
                item = current_sheet.iloc[iRow, iCol]
                if pandas.isna(item) or not item:
                    item = ""
                elif isinstance(item, pandas.Timestamp):
                    item = item.strftime('%Y-%m-%d %H:%M')
                temp.append(item)
            row_list.append(temp)

        title = row_list[0]  # 获取第一行表头
        for index in range(1, len(row_list)):
            jsonLine = dict(zip(title, row_list[index]))
            if "pt" in jsonLine and jsonLine["pt"] and type(jsonLine["pt"]) is not int:
                jsonLine["pt"] = get_time_stamp10(jsonLine["pt"], "%Y/%m/%d %H:%M")
            yield jsonLine
