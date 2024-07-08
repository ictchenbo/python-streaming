# -*- coding: utf-8 -*-
"""
@File    : write_mysql.py
@Author : yaojianlin(yaojianlin@golaxy.cn)
@Time : 2021/1/10 19:19
"""
import time
import sys

from conf import MYSQL_DATABASE_PERMISSION, MYSQL_DATABASE_BUZ, NAME_PLACEHOLDER
from database import get_mysql_app
from util.log_util import log

# 权限系统相关的  最主要是user表
mysql_app_permission = get_mysql_app(MYSQL_DATABASE_PERMISSION)
# 业务系统相关的  数据包的表
mysql_app_buz = get_mysql_app(MYSQL_DATABASE_BUZ)


def save_to_mysql(user_name, dataset_params, doc_nums, source=1, already_dataset_id=None, user_id=None):
    if user_id is None:
        # 系统中不存在此用户
        user_id = mysql_app_permission.get_userId_by_name(user_name)
        if user_id is None:
            log().info(f"系统中不存在此用户:::::{user_name}")
            sys.exit()
        # 更新到已有数据包
    if already_dataset_id is None:
        # 将元数据插入到mysql中  返回userid 和 数据包id
        user_id, dataset_id = save_data_to_mysql(user_id, user_name, dataset_params, doc_nums, source, True)
    else:
        # 判断数据库中有没有要添加的数据包
        _tmp_already_dataset_id = mysql_app_buz.get_datasetId(already_dataset_id)
        log().info("_tmp_already_dataset_id:::::::::::::" + str(_tmp_already_dataset_id))
        if _tmp_already_dataset_id is None:
            # 数据库中没有这个数据包Id 直接添加一个 但是不能是输入的数据包id因为主键是自增的
            user_id, dataset_id = save_data_to_mysql(user_id, user_name, dataset_params, doc_nums, source, True)
        else:
            # 添加已有数据包
            dataset_id = already_dataset_id
    return user_id, dataset_id


def save_data_to_mysql(user_id, user_name, dataset_params, doc_nums, source=1, is_parse: bool = True):
    """
    将关联的数据保存到mysql
    :param user_name:
    :param dataset_params:
    :param doc_nums:
    :param source:
    :param is_parse:
    :return: 返回user_id 和 数据包id
    """
    # 入mysql 只是插入基本信息，索引信息暂时不会插入，得到插入后id后在更新es_index字段
    create_date = time.strftime("%Y-%m-%d", time.localtime())
    dataset_record = (dataset_params["dataset_name"], dataset_params["des"], user_id,
                      user_name, source, create_date, create_date,
                      '', doc_nums)
    res, dataset_id = mysql_app_buz.insert_dataset_record(dataset_record)
    log().info("数据包id为：：：：%s" % (dataset_id))
    place_holder = "_" + NAME_PLACEHOLDER + "_"
    if is_parse:
        index = dataset_params["dataset_name"] + place_holder + str(dataset_id)
    else:
        # 目前离线上传的是关于实体的
        index = dataset_params["dataset_name"] + "_entity" + place_holder + str(dataset_id)
    # 更新关联的数据包
    mysql_app_buz.update_dataset_index(dataset_id, index)
    user_ref_dataset_record = (dataset_id, user_id, 0)
    mysql_app_buz.insert_dataset_user(user_ref_dataset_record)
    return user_id, dataset_id
