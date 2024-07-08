# -*-coding:utf-8-*-
import csv
import os
import json
import time
import codecs
from conf import DEAL_STRUCTURED_OR_UNSTRUCTURED_FILE_PATH, CREATER, CSV_FILE_PATH

'''
将json文件转换为csv文件
'''


def jsontocsv2(path):
    keys = ["type", "data_center_id", "resource_id", "resource_name", "creator", "create_time", "department",
            "sensibility_level", "resource_content"]

    jsonData = codecs.open(path, 'r', 'utf-8')
    csvfile = open(path + '.csv', 'w', newline='', encoding='utf-8')  # python3下
    writer = csv.writer(csvfile, delimiter='\001')
    writer.writerow(keys)  # 将属性列表写入csv中
    for line in jsonData:
        try:
            dataDic = {}
            rec = json.loads(line)
            if len(rec["_id"]) <= 0 or len(rec["cont"]) <= 0:
                continue
            dataDic["type"] = "au"
            dataDic["data_center_id"] = ""
            dataDic["resource_id"] = rec["_id"]
            dataDic["resource_name"] = rec["title"]
            dataDic["creator"] = rec["author"]
            dataDic["create_time"] = rec["pt"]
            dataDic["department"] = ""
            dataDic["sensibility_level"] = ""
            dataDic["resource_content"] = rec["cont"]
            # 读取json数据的每一行，将values数据一次一行的写入csv中
            writer.writerow(list(dataDic.values()))
        except Exception as e:
            continue
    jsonData.close()
    csvfile.close()


def jsontocsv(dataset_id):
    dataset_id = str(dataset_id)
    keys = ["type", "data_center_id", "resource_id", "resource_name", "creator", "create_time", "department",
            "sensibility_level", "resource_content"]
    csvfile = open(CSV_FILE_PATH + "/" + dataset_id + '.csv', 'w', newline='', encoding='utf-8')  # python3下
    writer = csv.writer(csvfile, delimiter='\001')
    writer.writerow(keys)  # 将属性列表写入csv中
    for root, dirs, files in os.walk(DEAL_STRUCTURED_OR_UNSTRUCTURED_FILE_PATH):
        for file in files:
            jsonData = codecs.open(os.path.join(root, file), 'r', 'utf-8')
            for line in jsonData:
                try:
                    dataDic = {}
                    rec = json.loads(line)
                    _id = rec['_id'] if "_id" in rec else ""
                    content = rec['content'] if "content" in rec else ""
                    if not _id and not content:
                        continue
                    dataDic["type"] = "au"
                    dataDic["data_center_id"] = ""
                    dataDic["resource_id"] = dataset_id + "_" + _id
                    dataDic["resource_name"] = rec["title"] if "title" in rec else ""
                    dataDic["creator"] = CREATER
                    dataDic["create_time"] = int(time.time())
                    dataDic["department"] = ""
                    dataDic["sensibility_level"] = ""
                    dataDic["resource_content"] = content
                    # 读取json数据的每一行，将values数据一次一行的写入csv中
                    writer.writerow(list(dataDic.values()))
                except Exception as e:
                    continue
            jsonData.close()
    csvfile.close()


if __name__ == '__main__':
    # path为文件路径，不需要填写后缀
    path1 = "./data/news_convert_bjhealthyNews"
    path2 = "./data/news_convert_chinahealthyNews"
    jsontocsv2(path1)
    jsontocsv2(path2)
