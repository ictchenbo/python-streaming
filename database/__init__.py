# -*- coding: utf-8 -*-
"""
@File : __init__.py.py

@Author : yaojianlin(yaojianlin@golaxy.cn)
@Time : 2021/1/10 19:17
"""
from conf import MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASS
from .mongo_application import MongoApplication
from .mysql_application import MysqlApplication


def get_mysql_app(database, host=MYSQL_HOST, port=MYSQL_PORT, user=MYSQL_USER, passwd=MYSQL_PASS):
    mysql_app = MysqlApplication(host, port, user, passwd, database)
    return mysql_app
