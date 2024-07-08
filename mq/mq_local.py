# -*- coding: utf-8 -*-
# @Time    : 2021/8/8 15:28
# @Author  : chenbo

from multiprocessing import Manager
import threading
import queue

from mq.mq_base import MqServer


class MqLocal(MqServer):
    """
    用于本地测试联调的消息队列 支持多进程通信
    """

    def __init__(self, config):
        self.manager = Manager()
        self.mq = self.manager.dict()

    def mq_writer(self, table, item):
        # print("write", table, item)
        if table not in self.mq:
            d = self.manager.dict()
            d[item["_id"]] = item
            self.mq[table] = d
        else:
            self.mq[table][item["_id"]] = item

    def mq_reader(self, table, limit=1):
        if table in self.mq and self.mq[table]:
            for k in self.mq[table].keys():
                item = self.mq[table][k]
                # print("read", table, item)
                return item
        return None

    def mq_delete(self, table, key):
        if table in self.mq and self.mq[table] and key in self.mq[table]:
            # print("delete", table, key)
            return self.mq[table].pop(key)
        return None


class MqQueue(MqServer):
    """
    用于本地测试联调的消息队列，仅用于单进程内的多线程
    """
    def __init__(self, config):
        self.config = config
        self.mutex = threading.Lock()
        self.queues = {}

    def mq_writer(self, table, item):
        if table not in self.queues:
            self.mutex.acquire()
            if table not in self.queues:
                self.queues[table] = queue.Queue(self.config.get("size", 1000))
            self.mutex.release()
        q = self.queues[table]
        q.put(item)

    def mq_reader(self, table, limit=1):
        if table not in self.queues:
            return None
        return self.queues[table].get()

    def mq_delete(self, table, key):
        pass
