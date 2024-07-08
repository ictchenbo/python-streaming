# -*- coding: utf-8 -*-
# @Time    : 2021/8/8 15:28
# @Author  : chenbo

import requests
import time

DOCUMENG_MAPPING = {
    "publist_time": "发布时间",
    "keywords": "keywords",
    "sentences": "倾向性"
}


def date_to_timestamp(date, format_string="%Y-%m-%d %H:%M:%S"):
    """
    将时间字符串转换为10位时间戳，时间字符串默认为2017-10-01 13:37:04格式
    :param date:
    :param format_string:
    :return:
    """
    time_array = time.strptime(date, format_string)
    time_stamp = int(time.mktime(time_array))
    return time_stamp


def mongo_writer(data, config):
    headers = {"Content-Type": "application/json; charset=utf-8", "userid": config["user_id"]}
    for item in data:
        item["id"] = config["dataset_id"]
        item["name"] = config["dataset_name"]
        res = requests.post(url=config["service"], json=item, headers=headers)
        print(res.json())


def read_mongo(config):
    from database.mongo_application import MongoApplication
    mongo_config = config["mongo"]
    mongoApplication = MongoApplication(mongo_config["host"], int(mongo_config["port"]),
                                        mongo_config["user"], mongo_config["password"])
    page = 1
    page_size = 50
    while True:
        data, count = mongoApplication.findByPager("goin_data_new1", "data3", {}, limit=page_size, page=page,
                                                   sort=[("_id", 1)])
        return_data = {"nodes": [], "edges": []}
        for data_item in data:
            for node_item in data_item["nodes"]:
                if node_item["metaType"] == "document":
                    node_item["time"] = date_to_timestamp(
                        node_item.get(DOCUMENG_MAPPING["publist_time"])) if node_item.get(
                        DOCUMENG_MAPPING["publist_time"]) else ""
                    node_item["keywords"] = node_item[DOCUMENG_MAPPING["keywords"]]
                    node_item["sentences"] = node_item[DOCUMENG_MAPPING["sentences"]]
            return_data["nodes"].extend(data_item["nodes"])
            return_data["edges"].extend(data_item["edges"])
        for item in return_data.get("nodes", []) + return_data.get("edges", []):
            if isinstance(item.get("time", []), list):
                item["time"] = ""
        mongo_writer([return_data], config['kg_instance'])
        if len(data) < page_size:
            break
        page += 1


# def load(data, config):
#     """
#     将数据写到kg-instance里
#     :param data:
#     :param config:
#     :return:
#     """
#     service = config.get("service")
#
#     if isinstance(data, list):
#         for date_item in data:
#             for graph_item in date_item:
#                 for item in graph_item.get("nodes", []) + graph_item.get("edges", []):
#                     if isinstance(item.get("time", []), list):
#                         item["time"] = ""
#                 res = requests.post(url=service, json=graph_item)
#                 print(res.json())
#     else:
#         print("data not list")
#     return True


def load(data, config):
    """
    将数据写到kg-instance里
    :param data:
    :param config:
    :return:
    """
    service = config.get("service")
    headers = {"Content-Type": "application/json; charset=utf-8", "userid": config["user_id"]}
    data["id"] = config["dataset_id"]
    data["name"] = config["dataset_name"]
    for node_item in data["nodes"]:
        if node_item.get("metaType", "") == "document":
            node_item["time"] = date_to_timestamp(
                node_item.get(DOCUMENG_MAPPING["publist_time"])) if node_item.get(
                DOCUMENG_MAPPING["publist_time"]) else ""
            node_item["keywords"] = node_item[DOCUMENG_MAPPING["keywords"]]
            node_item["sentences"] = node_item[DOCUMENG_MAPPING["sentences"]]
    res = requests.post(url=service, json=data, headers=headers)
    if not str(res.status_code).startswith("2"):
        raise Exception(res.content)
