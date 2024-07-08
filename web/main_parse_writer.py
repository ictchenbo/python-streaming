# -*- coding: utf-8 -*-
# @Time    : 2021/8/8 15:28
# @Author  : chenbo

import datetime
import os
import _thread

from flask import request

from util.result_util import asJson
from rest_app_base import app
from conf import PREFIX, USER_NAME, USER_ID, STRUCTURED_OR_UNSTRUCTURED_FILE_PATH, \
    DEAL_STRUCTURED_OR_UNSTRUCTURED_FILE_PATH
from util.log_util import log
from writer.tmp.write_mysql import save_to_mysql
from model.config_model import FileConfigModel, MySqlConfigModel
from database.mongo_application import MongoApplication
from web import parse_writer_task

'''
将文件（doc，docx，txt，pdf）录入到goin
'''


@app.route(PREFIX + "/file_parse_writer", methods=["POST"])
def file_parse_writer():
    try:
        config_model = FileConfigModel()

        # dataset_id:数据包ID，dataset_name：数据包名称，dataset_desc：数据包描述,is_parser_title:是否解析title
        # mongo_meta_dbname：要写入的mongo库，mongo_meta_table：要写入的mongo表,from_file_path:内容来源目录
        config_model.dataset_id = request.get_json().get("dataset_id", -1)
        config_model.dataset_name = request.get_json().get("dataset_name", "")
        config_model.dataset_desc = request.get_json().get("dataset_desc", "")
        # 原始数据-存储库、CoreNLU解析-数据来源库、sqlgraph入库-数据来源库
        config_model.mongo_meta_host = request.get_json().get("mongo_meta_host", "10.20.8.31")
        config_model.mongo_meta_port = request.get_json().get("mongo_meta_port", 27017)
        config_model.auth_meta = request.get_json().get("auth_meta", True)
        config_model.mongo_meta_user = request.get_json().get("mongo_meta_user", "admin")
        config_model.mongo_meta_passwd = request.get_json().get("mongo_meta_passwd", "123456")
        config_model.mongo_meta_dbname = request.get_json().get("mongo_meta_dbname", "")
        config_model.mongo_meta_table = request.get_json().get("mongo_meta_table", "")
        config_model.is_parser_title = request.get_json().get("is_parser_title", False)
        config_model.from_file_path = STRUCTURED_OR_UNSTRUCTURED_FILE_PATH + (
            request.get_json().get("from_file_path", ""))
        config_model.lang = request.get_json().get("lang", "zh")  # 中文改为: zh，英文：en

        # _nlu_topic：主题分类、_nlu_sentiment：情感分类、_nlu_event：事件识别、_nlu_nel：实体链接、
        # _nlu_chunk：词性归并、_nlu_tokens：分词、_nlu_sentences：分句、_nlu_pos：词性标注、_nlu_ner：实体识别

        # sqlGraph入库服务地址及配置信息(填写data-writer地址)
        config_model.data_writer_service = request.get_json().get("data_writer_service",
                                                                  "http://10.20.8.36:5000/converted_insert")
        # 将数据写入到某用户下面,默认用超级管理员id
        config_model.user_id = request.get_json().get("user_id", "ff8d3b07c3f1db80e1443e7b65836fba")
        config_model.user_name = request.get_json().get("user_name", "goin_admin")
        # 生成新的 DATASET_ID 库配置及查询配置
        config_model.mysql_host = request.get_json().get("mysql_host", "10.20.8.34")
        config_model.mysql_port = request.get_json().get("mysql_port", 3307)
        config_model.mysql_user = request.get_json().get("mysql_user", "root")
        config_model.mysql_pass = request.get_json().get("mysql_pass", "cas_goin")
        config_model.mysql_database_permission = request.get_json().get("mysql_database_permission", "cas_goin")
        config_model.mysql_database_buz = request.get_json().get("mysql_database_buz", "goin")
        config_model.name_placeholder = request.get_json().get("name_placeholder", "3")
        config_model.logger_level = request.get_json().get("logger_level", "info")

        # CoreNLU解析服务地址及配置信息
        config_model.nlu_parser_service = request.get_json().get("nlu_parser_service",
                                                                 "http://117.160.193.19:9080/nlu_inte")
        config_model.is_write_to_json = request.get_json().get("is_write_to_json", True)
        config_model.is_parse_all = request.get_json().get("is_parse_all", True)
        config_model.is_insert_mongo = request.get_json().get("is_insert_mongo", True)
        config_model.is_insert_sqlgraph = request.get_json().get("is_insert_sqlgraph", True)
        config_model.to_file = request.get_json().get("to_file", "")  # 要写入的文件，如果不指定则自动根据规则生成文件

        if not config_model.dataset_name:
            return asJson(status=40000, msg="dataset_name Parameter error")
        if not config_model.mongo_meta_dbname:
            return asJson(status=40000, msg="mongo_meta_dbname Parameter error")
        if not config_model.mongo_meta_table:
            return asJson(status=40000, msg="mongo_meta_table Parameter error")
        if config_model.from_file_path and not os.path.isdir(config_model.from_file_path):
            return asJson(status=40000, msg="from_file_path not exist")
        if not os.path.isdir(DEAL_STRUCTURED_OR_UNSTRUCTURED_FILE_PATH):
            os.mkdir(DEAL_STRUCTURED_OR_UNSTRUCTURED_FILE_PATH)
        if config_model.to_file == "":
            config_model.to_file = DEAL_STRUCTURED_OR_UNSTRUCTURED_FILE_PATH + (
                    datetime.datetime.now().strftime("%Y%m%d%H%M") + "_" + config_model.dataset_name)
        log().info("-------------begin----------------")
        # 0、获取数据包，若为-1则自动生成
        dataset_params = {"dataset_name": config_model.dataset_name, "des": config_model.dataset_desc}
        if config_model.dataset_id == -1:
            config_model.user_id, config_model.dataset_id = save_to_mysql(USER_NAME, dataset_params,
                                                                          doc_nums=0, source=1,
                                                                          already_dataset_id=None,
                                                                          user_id=USER_ID)
        else:
            old_dataset_id = config_model.dataset_id
            config_model.user_id, config_model.dataset_id = save_to_mysql(USER_NAME, dataset_params,
                                                                          doc_nums=0, source=1,
                                                                          already_dataset_id=config_model.dataset_id,
                                                                          user_id=USER_ID)
            if old_dataset_id != config_model.dataset_id:  # 如果传递的数据包和新生成的数据包Id不一致，更新mongo库里面的dataset_id。
                query = {"dataset_id": old_dataset_id}
                newvalues = {"dataset_id": config_model.dataset_id}
                mongoApplication = MongoApplication(config_model.mongo_meta_host,
                                                    int(config_model.mongo_meta_port),
                                                    config_model.mongo_meta_user,
                                                    config_model.mongo_meta_passwd,
                                                    config_model.auth_meta)

                mongoApplication.update_all_document(query, newvalues, config_model.mongo_meta_dbname,
                                                     config_model.mongo_meta_table)

        log().info(
            "dataset_id: {}, dataset_name: {}, des: {}".format(config_model.dataset_id, config_model.dataset_name,
                                                               config_model.dataset_desc))
        _thread.start_new_thread(parse_writer_task.file_parse_writer_task, (config_model,))
        return "file_parse_writer task running."
    except Exception as e:
        log().exception(e)
        return "file_parse_writer task error"


'''
将mysql数据录入goin
'''


@app.route(PREFIX + "/mysql_parse_writer", methods=["POST"])
def mysql_parse_writer():
    try:
        config_model = MySqlConfigModel()
        # dataset_id:数据包ID，dataset_name：数据包名称，dataset_desc：数据包描述,is_parser_title:是否解析title
        # mongo_meta_dbname：要写入的mongo库，mongo_meta_table：要写入的mongo表
        config_model.dataset_id = request.get_json().get("dataset_id", -1)
        config_model.dataset_name = request.get_json().get("dataset_name", "")
        config_model.dataset_desc = request.get_json().get("dataset_desc", "")
        # 原始数据-存储库、CoreNLU解析-数据来源库、sqlgraph入库-数据来源库
        config_model.mongo_meta_host = request.get_json().get("mongo_meta_host", "10.20.8.31")
        config_model.mongo_meta_port = request.get_json().get("mongo_meta_port", 27017)
        config_model.auth_meta = request.get_json().get("auth_meta", True)
        config_model.mongo_meta_user = request.get_json().get("mongo_meta_user", "admin")
        config_model.mongo_meta_passwd = request.get_json().get("mongo_meta_passwd", "123456")
        config_model.mongo_meta_dbname = request.get_json().get("mongo_meta_dbname", "")
        config_model.mongo_meta_table = request.get_json().get("mongo_meta_table", "")
        config_model.is_parser_title = request.get_json().get("is_parser_title", False)

        config_model.lang = request.get_json().get("lang", "zh")  # 中文改为: zh，英文：en

        # _nlu_topic：主题分类、_nlu_sentiment：情感分类、_nlu_event：事件识别、_nlu_nel：实体链接、
        # _nlu_chunk：词性归并、_nlu_tokens：分词、_nlu_sentences：分句、_nlu_pos：词性标注、_nlu_ner：实体识别
        config_model.cont_tasks = request.get_json().get("cont_tasks",
                                                         ["_nlu_pos", "_nlu_chunk", "_nlu_ner", "_nlu_nel",
                                                          "_nlu_topic",
                                                          "_nlu_keywords", "_nlu_event", "_nlu_sentiment",
                                                          "_nlu_tokens",
                                                          "_nlu_sentences"])
        # sqlGraph入库服务地址及配置信息(填写data-writer地址)
        config_model.data_writer_service = request.get_json().get("data_writer_service",
                                                                  "http://10.20.8.36:5000/converted_insert")
        # 将数据写入到某用户下面,默认用超级管理员id
        config_model.user_id = request.get_json().get("user_id", "ff8d3b07c3f1db80e1443e7b65836fba")
        config_model.user_name = request.get_json().get("user_name", "goin_admin")
        # 生成新的 DATASET_ID 库配置及查询配置
        config_model.mysql_host = request.get_json().get("mysql_host", "10.20.8.34")
        config_model.mysql_port = request.get_json().get("mysql_port", 3307)
        config_model.mysql_user = request.get_json().get("mysql_user", "root")
        config_model.mysql_pass = request.get_json().get("mysql_pass", "cas_goin")
        config_model.mysql_database_permission = request.get_json().get("mysql_database_permission", "cas_goin")
        config_model.mysql_database_buz = request.get_json().get("mysql_database_buz", "goin")
        config_model.name_placeholder = request.get_json().get("name_placeholder", "3")
        config_model.logger_level = request.get_json().get("logger_level", "info")

        # CoreNLU解析服务地址及配置信息
        config_model.nlu_parser_service = request.get_json().get("nlu_parser_service",
                                                                 "http://117.160.193.19:9080/nlu_inte")
        config_model.is_write_to_json = request.get_json().get("is_write_to_json", True)
        config_model.is_parse_all = request.get_json().get("is_parse_all", True)
        config_model.is_insert_mongo = request.get_json().get("is_insert_mongo", True)
        config_model.is_insert_sqlgraph = request.get_json().get("is_insert_sqlgraph", True)
        config_model.to_file = request.get_json().get("to_file", "")  # 要写入的文件，如果不指定则自动根据规则生成文件
        # mysql数据来源
        config_model.mysql_ip = request.get_json().get("mysql_ip", "")
        config_model.mysql_port = request.get_json().get("mysql_port", 3307)
        config_model.mysql_user = request.get_json().get("mysql_user", "")
        config_model.mysql_passwd = request.get_json().get("mysql_passwd", "")
        config_model.mysql_limit = request.get_json().get("mysql_limit", 10)
        config_model.mysql_db = request.get_json().get("mysql_db", "")
        config_model.mysql_table = request.get_json().get("mysql_table", "")

        if not config_model.dataset_name:
            return asJson(status=40000, msg="dataset_name Parameter error")
        if not config_model.mongo_meta_dbname:
            return asJson(status=40000, msg="mongo_meta_dbname Parameter error")
        if not config_model.mongo_meta_table:
            return asJson(status=40000, msg="mongo_meta_table Parameter error")
        if not config_model.mysql_ip:
            return asJson(status=40000, msg="mysql_ip Parameter error")
        if not config_model.mysql_user:
            return asJson(status=40000, msg="mysql_user Parameter error")
        if not config_model.mysql_passwd:
            return asJson(status=40000, msg="mysql_passwd Parameter error")
        if not config_model.mysql_db:
            return asJson(status=40000, msg="mysql_db Parameter error")
        if not config_model.mysql_table:
            return asJson(status=40000, msg="mysql_table Parameter error")

        if not os.path.isdir(DEAL_STRUCTURED_OR_UNSTRUCTURED_FILE_PATH):
            os.mkdir(DEAL_STRUCTURED_OR_UNSTRUCTURED_FILE_PATH)
        if config_model.to_file == "":
            config_model.to_file = DEAL_STRUCTURED_OR_UNSTRUCTURED_FILE_PATH + (
                    datetime.datetime.now().strftime("%Y%m%d%H%M") + "_" + config_model.dataset_name)
        log().info("-------------begin----------------")
        # 0、获取数据包，若为-1则自动生成
        dataset_params = {"dataset_name": config_model.dataset_name, "des": config_model.dataset_desc}
        if config_model.dataset_id == -1:
            config_model.user_id, config_model.dataset_id = save_to_mysql(USER_NAME, dataset_params,
                                                                          doc_nums=0, source=1,
                                                                          already_dataset_id=None,
                                                                          user_id=USER_ID)
        else:
            old_dataset_id = config_model.dataset_id
            config_model.user_id, config_model.dataset_id = save_to_mysql(USER_NAME, dataset_params,
                                                                          doc_nums=0, source=1,
                                                                          already_dataset_id=config_model.dataset_id,
                                                                          user_id=USER_ID)
            if old_dataset_id != config_model.dataset_id:  # 如果传递的数据包和新生成的数据包Id不一致，更新mongo库里面的dataset_id。
                query = {"dataset_id": old_dataset_id}
                newvalues = {"dataset_id": config_model.dataset_id}
                mongoApplication = MongoApplication(config_model.mongo_meta_host,
                                                    int(config_model.mongo_meta_port),
                                                    config_model.mongo_meta_user,
                                                    config_model.mongo_meta_passwd,
                                                    config_model.auth_meta)

                mongoApplication.update_all_document(query, newvalues, config_model.mongo_meta_dbname,
                                                     config_model.mongo_meta_table)

        log().info(
            "dataset_id: {}, dataset_name: {}, des: {}".format(config_model.dataset_id, config_model.dataset_name,
                                                               config_model.dataset_desc))
        _thread.start_new_thread(parse_writer_task.mysql_parse_writer_task, (config_model,))
        return "mysql_parse_writer task running."
    except Exception as e:
        log().exception(e)
        return "mysql_parse_writer task error"


'''
将pgsql数据录入goin
'''


@app.route(PREFIX + "/pgsql_parse_writer", methods=["POST"])
def pgsql_parse_writer():
    try:
        config_model = MySqlConfigModel()
        # dataset_id:数据包ID，dataset_name：数据包名称，dataset_desc：数据包描述,is_parser_title:是否解析title
        # mongo_meta_dbname：要写入的mongo库，mongo_meta_table：要写入的mongo表
        config_model.dataset_id = request.get_json().get("dataset_id", -1)
        config_model.dataset_name = request.get_json().get("dataset_name", "")
        config_model.dataset_desc = request.get_json().get("dataset_desc", "")
        # 原始数据-存储库、CoreNLU解析-数据来源库、sqlgraph入库-数据来源库
        config_model.mongo_meta_host = request.get_json().get("mongo_meta_host", "10.20.8.31")
        config_model.mongo_meta_port = request.get_json().get("mongo_meta_port", 27017)
        config_model.auth_meta = request.get_json().get("auth_meta", True)
        config_model.mongo_meta_user = request.get_json().get("mongo_meta_user", "admin")
        config_model.mongo_meta_passwd = request.get_json().get("mongo_meta_passwd", "123456")
        config_model.mongo_meta_dbname = request.get_json().get("mongo_meta_dbname", "")
        config_model.mongo_meta_table = request.get_json().get("mongo_meta_table", "")
        config_model.is_parser_title = request.get_json().get("is_parser_title", False)

        config_model.lang = request.get_json().get("lang", "zh")  # 中文改为: zh，英文：en

        # _nlu_topic：主题分类、_nlu_sentiment：情感分类、_nlu_event：事件识别、_nlu_nel：实体链接、
        # _nlu_chunk：词性归并、_nlu_tokens：分词、_nlu_sentences：分句、_nlu_pos：词性标注、_nlu_ner：实体识别
        config_model.cont_tasks = request.get_json().get("cont_tasks",
                                                         ["_nlu_pos", "_nlu_chunk", "_nlu_ner", "_nlu_nel",
                                                          "_nlu_topic",
                                                          "_nlu_keywords", "_nlu_event", "_nlu_sentiment",
                                                          "_nlu_tokens",
                                                          "_nlu_sentences"])
        # sqlGraph入库服务地址及配置信息(填写data-writer地址)
        config_model.data_writer_service = request.get_json().get("data_writer_service",
                                                                  "http://10.20.8.36:5000/converted_insert")
        # 将数据写入到某用户下面,默认用超级管理员id
        config_model.user_id = request.get_json().get("user_id", "ff8d3b07c3f1db80e1443e7b65836fba")
        config_model.user_name = request.get_json().get("user_name", "goin_admin")
        # 生成新的 DATASET_ID 库配置及查询配置
        config_model.mysql_host = request.get_json().get("mysql_host", "10.20.8.34")
        config_model.mysql_port = request.get_json().get("mysql_port", 3307)
        config_model.mysql_user = request.get_json().get("mysql_user", "root")
        config_model.mysql_pass = request.get_json().get("mysql_pass", "cas_goin")
        config_model.mysql_database_permission = request.get_json().get("mysql_database_permission", "cas_goin")
        config_model.mysql_database_buz = request.get_json().get("mysql_database_buz", "goin")
        config_model.name_placeholder = request.get_json().get("name_placeholder", "3")
        config_model.logger_level = request.get_json().get("logger_level", "info")

        # CoreNLU解析服务地址及配置信息
        config_model.nlu_parser_service = request.get_json().get("nlu_parser_service",
                                                                 "http://117.160.193.19:9080/nlu_inte")
        config_model.is_write_to_json = request.get_json().get("is_write_to_json", True)
        config_model.is_parse_all = request.get_json().get("is_parse_all", True)
        config_model.is_insert_mongo = request.get_json().get("is_insert_mongo", True)
        config_model.is_insert_sqlgraph = request.get_json().get("is_insert_sqlgraph", True)
        config_model.to_file = request.get_json().get("to_file", "")  # 要写入的文件，如果不指定则自动根据规则生成文件
        # pgsql数据来源
        config_model.pgsql_ip = request.get_json().get("pgsql_ip", "")
        config_model.pgsql_port = request.get_json().get("pgsql_port", 3307)
        config_model.pgsql_user = request.get_json().get("pgsql_user", "")
        config_model.pgsql_passwd = request.get_json().get("pgsql_passwd", "")
        config_model.pgsql_limit = request.get_json().get("pgsql_limit", 10)
        config_model.pgsql_db = request.get_json().get("pgsql_db", "")
        config_model.pgsql_table = request.get_json().get("pgsql_table", "")

        if not config_model.dataset_name:
            return asJson(status=40000, msg="dataset_name Parameter error")
        if not config_model.mongo_meta_dbname:
            return asJson(status=40000, msg="mongo_meta_dbname Parameter error")
        if not config_model.mongo_meta_table:
            return asJson(status=40000, msg="mongo_meta_table Parameter error")
        if not config_model.pgsql_ip:
            return asJson(status=40000, msg="pgsql_ip Parameter error")
        if not config_model.pgsql_user:
            return asJson(status=40000, msg="pgsql_user Parameter error")
        if not config_model.pgsql_passwd:
            return asJson(status=40000, msg="pgsql_passwd Parameter error")
        if not config_model.pgsql_db:
            return asJson(status=40000, msg="pgsql_db Parameter error")
        if not config_model.pgsql_table:
            return asJson(status=40000, msg="pgsql_table Parameter error")

        if not os.path.isdir(DEAL_STRUCTURED_OR_UNSTRUCTURED_FILE_PATH):
            os.mkdir(DEAL_STRUCTURED_OR_UNSTRUCTURED_FILE_PATH)
        if config_model.to_file == "":
            config_model.to_file = DEAL_STRUCTURED_OR_UNSTRUCTURED_FILE_PATH + (
                    datetime.datetime.now().strftime("%Y%m%d%H%M") + "_" + config_model.dataset_name)
        log().info("-------------begin----------------")
        # 0、获取数据包，若为-1则自动生成
        dataset_params = {"dataset_name": config_model.dataset_name, "des": config_model.dataset_desc}
        if config_model.dataset_id == -1:
            config_model.user_id, config_model.dataset_id = save_to_mysql(USER_NAME, dataset_params,
                                                                          doc_nums=0, source=1,
                                                                          already_dataset_id=None,
                                                                          user_id=USER_ID)
        else:
            old_dataset_id = config_model.dataset_id
            config_model.user_id, config_model.dataset_id = save_to_mysql(USER_NAME, dataset_params,
                                                                          doc_nums=0, source=1,
                                                                          already_dataset_id=config_model.dataset_id,
                                                                          user_id=USER_ID)
            if old_dataset_id != config_model.dataset_id:  # 如果传递的数据包和新生成的数据包Id不一致，更新mongo库里面的dataset_id。
                query = {"dataset_id": old_dataset_id}
                newvalues = {"dataset_id": config_model.dataset_id}
                mongoApplication = MongoApplication(config_model.mongo_meta_host,
                                                    int(config_model.mongo_meta_port),
                                                    config_model.mongo_meta_user,
                                                    config_model.mongo_meta_passwd,
                                                    config_model.auth_meta)

                mongoApplication.update_all_document(query, newvalues, config_model.mongo_meta_dbname,
                                                     config_model.mongo_meta_table)

        log().info(
            "dataset_id: {}, dataset_name: {}, des: {}".format(config_model.dataset_id, config_model.dataset_name,
                                                               config_model.dataset_desc))
        _thread.start_new_thread(parse_writer_task.pgsql_parse_writer_task, (config_model,))
        return "pgsql_parse_writer task running."
    except Exception as e:
        log().exception(e)
        return "pgsql_parse_writer task error"
