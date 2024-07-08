# -*- coding: utf-8 -*-
# @Time    : 2021/8/2 18:20
# @Author  : shilei


from database.mongo_application import MongoApplication
from mq.mq_base import MqServer


class MqMongo(MqServer):
    def __init__(self, config):
        self.config = config
        user = config.get("user")
        passwd = config.get("passwd")
        auth = bool(user)
        self.mongoApplication = MongoApplication(
            config["host"], int(config["port"]), user=user, passwd=passwd, ifauth=auth)
        self.db = config.get("db", "goin_mq")

    def mq_writer(self, table, item):
        self.mongoApplication.insert_one(self.db, table, item)

    def mq_reader(self, table, limit=1):
        if limit > 1:
            rows, total = self.mongoApplication.findByPager(self.db, table, limit=limit)
            return rows
        result = self.mongoApplication.find_one(self.db, table)
        return result

    def mq_delete(self, table, id):
        result = self.mongoApplication.delete_one(self.db, table, id)
        return result

