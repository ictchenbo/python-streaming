# -*- coding: utf-8 -*-
# @Time    : 2021/8/8 15:28
# @Author  : chenbo

import json
from node_base import BaseNode


class load(BaseNode):
    """
        将数据输出到文件
    """
    def __init__(self, config):
        print("init writer.write_file:load")
        BaseNode.__init__(self, config)
        self.format = config.get("format", "jsonl")
        path = config.get("path")
        mode = config.get("open_mode", "append")
        encoding = config.get("encoding", "utf-8")
        self.fout = open(path, "a" if mode == "append" else "w", encoding=encoding)

    def load(self, data):
        if self.format == "jsonl" and isinstance(data, list):
            for row in data:
                self.write(row)
        else:
            self.write(data)
        return data

    def write(self, row):
        self.fout.write(json.dumps(row, ensure_ascii=False))
        self.fout.write("\n")

    def close(self):
        self.fout.close()
