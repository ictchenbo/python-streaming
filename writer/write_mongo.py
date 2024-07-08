# -*- coding: utf-8 -*-
# @Time    : 2021/8/8 15:28
# @Author  : chenbo

import json

from database.mongo_application import MongoApplication
from node_base import BaseNode


class load(BaseNode):
    def __init__(self, config):
        self.mongo = MongoApplication(config["host"], int(config["port"]),
                                        config["user"], config["password"])
        self.db = config["db"]
        self.table = config["table"]

    def load(self, data):
        try:
            self.mongo.insert(self.db, self.table, data)
        except Exception as e:
            print("writer mongo data {} error {}".format(data, e))
