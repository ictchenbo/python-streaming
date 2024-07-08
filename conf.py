# -*- coding: utf-8 -*-
# @Time    : 2021/8/8 15:28
# @Author  : chenbo

"""
@File : conf.py
配置文件
@Author : yaojianlin(yaojianlin@golaxy.cn)
@Time : 2021/1/10 16:23
"""
# web
APP_NAME = "parse_writer API"
PORT = 5555

# 接口统一前缀
PREFIX = "/parse_writer/v1.0"

# ################################## SERVICE ####################################
# CoreNLU解析服务地址及配置信息
TITLE_TASKS = ["_nlu_nel"]

# sqlGraph入库服务地址及配置信息(填写data-writer地址)
DATA_WRITER_SERVICE = 'http://10.20.8.36:5000/converted_insert'  # (填写goin-writer地址)
# 用户id，默认用超级管理员id
USER_ID = 'ff8d3b07c3f1db80e1443e7b65836fba'
USER_NAME = 'goin_admin'
# 限制条数
LIMIT = 20000

############################### mysql #######################
# 生成新的 DATASET_ID 库配置及查询配置
MYSQL_HOST = '10.20.8.34'
MYSQL_PORT = 3307
MYSQL_USER = 'root'
MYSQL_PASS = 'cas_goin'
MYSQL_DATABASE_PERMISSION = 'cas_goin'
MYSQL_DATABASE_BUZ = 'goin'
NAME_PLACEHOLDER = '3'
LOGGER_LEVEL = "info"

# 结构化和非结构化文件存放目录
STRUCTURED_OR_UNSTRUCTURED_FILE_PATH = "./data/structuredOrUnStructured/"
# 处理完:结构化和非结构化文件解析目录
DEAL_STRUCTURED_OR_UNSTRUCTURED_FILE_PATH = "./data/dealStructuredOrUnStructured/"

# 给客户的csv文件
CSV_FILE_PATH = './data/csv'
# 文件上传者
CREATER = "张三"

mq_config = {
    "type": "mongo",
    "mongo": {
        "host": "10.170.130.177",
        "port": "27017",
        "db": "goin_mq",
        "user": "admin",
        "passwd": "123456"
    }
}
