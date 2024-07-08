# -*- coding: utf-8 -*-
# @Time    : 2021/8/8 15:28
# @Author  : chenbo

"""
 * @Author: duyilin@golaxy.cn
 * @Date: Created in 14:40 2020/6/4
"""
from bson import ObjectId
from pymongo import MongoClient


class MongoApplication(object):
    def __init__(self, host, port, user=None, passwd=None, ifauth=True, **kwargs):
        self.client = MongoClient(host=host, port=port)
        if ifauth:
            assert user is not None and passwd is not None
            self.client.admin.authenticate(user, passwd)

    def getCollection(self, dbname, collname):
        db = self.client[dbname]
        return db[collname]

    def findByQuery(self, dbname: str, collname: str, query=None, limit=10):
        """
        查询某个collection中某个条件的全部记录
        :param dbname:
        :param collname:
        :param query:
        :return:
        """
        cl = self.getCollection(dbname, collname)
        result = cl.find(query, limit=limit)
        return list(result)

    def update_one_document(self, query, newvalues, dbname, collname):
        cl = self.getCollection(dbname, collname)
        if "_id" in newvalues:
            newvalues["_id"] = ObjectId(newvalues["_id"])
        if "$set" not in newvalues:
            newvalues = {"$set": newvalues}
        result = cl.update_one(query, newvalues)
        return result.modified_count

    def update_all_document(self, query, newvalues, dbname, collname):
        cl = self.getCollection(dbname, collname)
        if "_id" in newvalues:
            newvalues["_id"] = ObjectId(newvalues["_id"])
        if "$set" not in newvalues:
            newvalues = {"$set": newvalues}
        result = cl.update_many(query, newvalues)
        return result.modified_count

    def update_faield_num(self, query, newvalues, dbname, collname):
        cl = self.getCollection(dbname, collname)
        result = cl.find(query)
        if list(result) == 0:
            result = cl.insert_many([query])
            return len(result.inserted_ids)
        else:
            newvalues = {"$inc": newvalues}
            result = cl.update_one(query, newvalues)
            return result.modified_count

    def incr_(self, query, newvalues, dbname, collname):
        cl = self.getCollection(dbname, collname)
        newvalues = {"$inc": newvalues}
        result = cl.update_one(query, newvalues)
        return result.modified_count

    def findByPager(self, dbname: str, collname: str, query=None, limit=10, page=1, sort=None):
        """
        分页查询
        :param dbname: 数据库名字
        :param collname: 集合（表名）
        :param query: 查询条件
        :param limit: -1 表示查询全部
        :param page: 页码 从0开始
        :param sort:  默认按照 create_time 降序排序
        :return:
        """
        cl = self.getCollection(dbname, collname)
        if limit <= 0:
            result = cl.find(query)
        else:
            result = cl.find(query).limit(limit).skip((page - 1) * limit)
        if sort:
            result = result.sort(sort)
        total = cl.find(query).count()
        return list(result), total

    def find_one(self, dbname, collname):
        cl = self.getCollection(dbname, collname)
        return cl.find_one()

    def inset_one(self, dbname, collname, data):
        cl = self.getCollection(dbname, collname)
        return cl.find_one()

    def findByPager1(self, dbname: str, collname: str, query=None, limit=10, page=1, pageSize=10,
                     sort=None):
        """
        分页查询
        :param dbname: 数据库名字
        :param collname: 集合（表名）
        :param query: 查询条件
        :param limit: -1 表示查询全部
        :param page: 页码 从0开始
        :param pageSize: 每页显示多少条
        :param sort:  默认按照 create_time 降序排序
        :return:
        """
        cl = self.getCollection(dbname, collname)
        if limit == -1:
            result = cl.find(query).sort(sort)
        else:
            result = cl.find(query).sort(sort).limit(pageSize).skip(page * pageSize)
        total = len(list(cl.find(query).sort(sort)))
        return list(result), total

    def find_by_page(self, dbname: str, collname: str, query=None, window_page_params=None, inner_page_params=None):
        """
        分页查询
        :return:
        """
        cl = self.getCollection(dbname, collname)
        if window_page_params is not None:
            # sort = window_page_params["sort"]
            page = window_page_params["page"]
            pageSize = window_page_params["pageSize"]
            limit = window_page_params["limit"]
            if limit == -1:
                result = cl.find(query)  # .sort(sort)
            else:
                result = cl.find(query).limit(pageSize).skip((page - 1) * pageSize)

            res = list(result)
            return res

    def insert(self, dbname, collname, rows: list):
        cl = self.getCollection(dbname, collname)
        return cl.insert_many(rows)

    def insert_one(self, dbname, collname, row):
        cl = self.getCollection(dbname, collname)
        return cl.insert_one(row)

    def delete_manay(self, dbname, collname, filter):
        """
        删除数据
        :param dbname:
        :param collname:
        :param filter:
        :return:
        """
        cl = self.getCollection(dbname, collname, id)
        return cl.delete_many(filter)

    def delete_one(self, dbname, collname, id):
        cl = self.getCollection(dbname, collname)
        myquery = {"_id": id}
        cl.delete_one(myquery)

    def save(self, dbname, collname, rows: dict):
        """
        保存
        :param rows:
        :param collname:
        :param dbname:
        :return:
        """
        cl = self.getCollection(dbname, collname)
        return cl.save(rows)
