# -*- coding: utf-8 -*-
# @Time    : 2021/8/8 15:28
# @Author  : chenbo

import time
from database.mongo_application import MongoApplication


def write_mongo(doc, config):
    from model.parse_cont_model import ParseContModel
    parse_cont_model: ParseContModel = config.get("config")
    mongo = MongoApplication(parse_cont_model.mongo_meta_host, int(parse_cont_model.mongo_meta_port),
                             parse_cont_model.mongo_meta_user, parse_cont_model.mongo_meta_passwd,
                             parse_cont_model.auth_meta)
    query = {"_id": doc["id"]}
    data_list = mongo.findByQuery(parse_cont_model.mongo_meta_dbname,
                                  parse_cont_model.mongo_meta_table, query, 1)
    if data_list:
        doc = data_list[0]
        doc.update({
            "update_time": int(time.time()),
            "_corenlu_stage": 9,
            '_nlu_cont': {},
            '_nlu_title': {}
        })
        mongo.update_one_document(query, doc, parse_cont_model.mongo_meta_dbname,
                                  parse_cont_model.mongo_meta_table)


def load(docs, config):
    return True
