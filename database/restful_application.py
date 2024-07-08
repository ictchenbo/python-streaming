# -*- coding: utf-8 -*-
# @Time    : 2021/8/8 15:28
# @Author  : chenbo

"""
@Author : yaojianlin(yaojianlin@golaxy.cn)
@Time : 2021/1/10 19:19
"""
import requests

requests.adapters.DEFAULT_RETRIES = 5


class BasicApplication(object):
    """sqlgraph的restful api"""

    def __init__(self, host: str, port: int or str, user: str, password: str, database: str):
        self.config = {
            "host": host,
            "port": port,
            "user": user,
            "password": password,
            "database": database
        }
        self.url = "http://{host}:{port}/".format(**self.config)

    @property
    def restful_url(self):
        if self.config["user"] is not None:
            return "http://{user}:{password}@{host}:{port}/{database}/".format(**self.config)
        else:
            return "http://{host}:{port}/{database}/".format(**self.config)

    @property
    def restful_url_post(self):
        if self.config["user"] is not None:
            return "http://{user}:{password}@{host}:{port}/".format(**self.config)
        else:
            return "http://{host}:{port}/".format(**self.config)

    def execute(self, query, format, insert=False):
        if not insert:
            if format == "":
                return requests.post(self.restful_url_post, params={"query": query})
            elif format == "JSON":
                data = (query + "format %s" % format).encode("utf-8")
                return requests.post(self.restful_url, data=data)
            elif format == "Graph":
                data = (query + "format %s" % format).encode("utf-8")
                return requests.post(self.restful_url_post, data=data)
        else:
            return requests.post(self.restful_url, data=query.encode("utf-8"))


if __name__ == '__main__':
    BasicApplication("10.60.1.148", "8124", "", "", "graph_200415_v_1_4")
