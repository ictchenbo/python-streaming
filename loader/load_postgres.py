# -*- coding: utf-8 -*-
# @Time    : 2021/8/8 15:28
# @Author  : chenbo

import psycopg2
import math
from util.log_util import log


def load(config, table, options={}):
    ip = config.get("ip", "localhost")
    port = config.get("port", 3306)
    user = config.get("user")
    passwd = config.get("password")
    database = config.get("database")
    page_count = options.get("page_count", 1000)

    if not ip or not port or not database or not table:
        log().info("ip: {}, port: {}, database: {}, table: {}".format(ip, port, database, table))
        log().info("ip, port, database, table have empty")
        return
    conn = psycopg2.connect(database=database,
                            user=user,
                            password=passwd,
                            host=ip,
                            port=port)
    cursor = conn.cursor()
    count_sql = 'select count(*) as totalCount from {0};'.format(table)
    cursor.execute(count_sql)
    data_count = cursor.fetchall()[0][0]

    index_count = 0
    for index in range(0, math.ceil(data_count / page_count)):
        select_sql = 'select *from {0} limit {1} offset {2};'.format(table, page_count, index_count)
        cursor.execute(select_sql)
        col_name_list = [tuple[0] for tuple in cursor.description]
        for item in cursor.fetchall():
            rec = dict(zip(col_name_list, list(item)))
            yield rec
        index_count = (index + 1) * page_count
    cursor.close()
    conn.close()
