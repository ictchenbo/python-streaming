# -*- coding: utf-8 -*-
# @Time    : 2021/8/16 14:20
# @Author  : shilei

import copy
from mq.mq_mongo import MqMongo
from conf import mq_config

mq_client = MqMongo(mq_config["mongo"])


def repeat_error(all_docs, task_config):
    repeat_count = task_config["repeat_count"]
    if not repeat_count:
        return
    for doc_item in all_docs:
        doc_item = copy.deepcopy(doc_item)
        if doc_item.get("error_count") is None:
            doc_item["error_count"] = 0
        if doc_item["error_count"] < repeat_count:
            doc_item['error_count'] += 1
            doc_item["_id"] = "z" + doc_item["_id"]  # id 排序放到最后
            mq_client.mq_writer("loader", doc_item)
