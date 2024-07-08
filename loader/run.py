# -*- coding: utf-8 -*-
# @Time    : 2021/8/8 15:28
# @Author  : chenbo

import os
import uuid

from module_manager import manager

structure_files = ["mysql", "postgres", "mongodb", "csv", "xls", "xlsx", "json"]
unstructure_files = ["pdf", "txt", "doc", "docx"]
ext_map = {
    "xlsx": "xls",
    "docx": "doc"
}


def add_id(doc, tag=True):
    """
    自动生成ID
    :param doc:
    :param tag:
    :return:
    """
    if tag and "_id" not in doc:
        doc["_id"] = str(uuid.uuid1().hex)
    return doc


def mv_file(filepath, target_dir):
    if target_dir:
        os.system("mv {} {}".format(filepath, target_dir))


def unstructure_data_file(filepath, name_as_title=True):
    """
    解析单个文件
    :param filepath: 文件路径
    :param name_as_title: 对于word/pdf/txt，如果标题抽取失败，将文件名作为文档标题
    :return:
    """
    print("loading", filepath)
    file_name = os.path.basename(filepath)
    ext = file_name[file_name.rfind(".") + 1:]
    # if ext not in structure_files and ext not in unstructure_files:
    #     pass
    #
    ext2 = ext_map.get(ext) or ext
    loader = manager.load("loader", f"load_{ext2}")

    for doc in loader(filepath):
        if name_as_title and ext in unstructure_files and "title" not in doc:
            pos = file_name.rfind(".")
            doc["title"] = file_name[:pos] if pos > 0 else file_name
        yield doc


def source_db(source_config):
    """
    数据库源
    :param source_config: 数据配置
    :return:
    """
    db_config = source_config.get("db")
    db_type = db_config.get("type")
    if db_type not in structure_files:
        return

    tables = db_config.get("table")
    if isinstance(tables, str):
        tables = [tables]
    loader = manager.load("loader", f"load_{db_type}")
    for table in tables:
        for doc in loader(db_config, table):
            yield doc


def source_file(file_config):
    """
    文件源
    :param file_config:
    :return:
    """
    path = file_config.get("path")
    if isinstance(path, str):
        path = [path]
    allow = file_config.get("allow", True)  # 默认是否处理
    includes = file_config.get("include")  # 文件后缀 白名单
    excludes = file_config.get("exclude")  # 文件后缀 黑名单

    bak_dir = file_config.get("bak_dir")

    def allow_file(file_name):
        """
        判断是否处理该文件，具体规则：
            1. 如果指定了include，则采用白名单机制，必须包含在include中；
            2. 如果指定了黑名单，则必须排出在黑名单；
            3. 否则看默认规则
        :param file_name: 文件名
        :return:
        """
        ext = file_name[file_name.rfind(".") + 1:]
        if includes:
            return ext in includes
        if excludes:
            return ext not in excludes
        return allow

    name_title = file_config.get("name_as_title", True)

    for file_path in path:
        if os.path.isfile(file_path) and allow_file(file_path):
            for doc in unstructure_data_file(file_path, name_title):
                yield doc
            mv_file(file_path, bak_dir)
            continue

        if not os.path.isdir(file_path):
            continue

        for root, dirs, files in os.walk(file_path):
            for file in files:
                if not allow_file(file):
                    continue
                for doc in unstructure_data_file(os.path.join(root, file), name_title):
                    yield doc
                mv_file(file_path, bak_dir)


def load(config, callback_fun):
    """
        加载非结构化数据，分为两种
        1. 非结构化文件：一个文件一条记录；如果没有提取到标题，则使用文件名作为标题
        2. 结构化来源：一行对应一条记录；要求至少有cont或content字段；其他字段全部保留或舍弃
        :return: 返回文档迭代器  {title, content, _id, * }
    """
    tag_add_id = config.get("add_id", True)

    source_config = config["source"]
    source_type = source_config.get("type")
    source_config_detail = source_config.get(source_type, {})
    if source_type == "db":
        data_gen = source_db(source_config_detail)
    else:
        data_gen = source_file(source_config_detail)
    for row in data_gen:
        row = add_id(row, tag_add_id)
        # print(row)
        callback_fun(row)
