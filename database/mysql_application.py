# -*- coding: utf-8 -*-
# @Time    : 2021/8/8 15:28
# @Author  : chenbo

"""
 * @Author: duyilin@golaxy.cn
 * @Date: Created in 16:53 2020/6/6
"""
import datetime

import pymysql
from util.log_util import log


class MysqlApplication(object):
    def __init__(self, host: str, port: int, user: int or str, password: str,
                 database: str, charset='utf8'):
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.charset = charset

    def connect(self):
        """获取连接对象和执行对象"""
        try:
            self.conn = pymysql.connect(host=self.host,
                                        port=self.port,
                                        user=self.user,
                                        password=self.password,
                                        database=self.database,
                                        charset=self.charset)
            self.cur = self.conn.cursor()
        except Exception as error:
            raise ConnectionError(error)

    def close(self):
        """关闭执行工具和连接对象"""
        if self.conn and self.cur:
            self.cur.close()
            self.conn.close()
        return True

    def delete(self, record: str):
        """Delete some record from the mysql database"""
        # try:
        #     response = self.cur.delete(record)
        # except Exception as error:
        #     raise error
        # return response
        return self.__item(record)

    def update(self, record: str):
        """
        Execute sql query and return nodes from SQLgraph format JSON response
        :param query: sql string
        :return: a response dict with nodes
        """
        # try:
        #     response = self.cur.update(record)
        # except Exception as error:
        #     raise error
        # return response
        return self.__item(record)

    def fetch_one(self, sql, params=None):
        '''
        根据sql和参数获取一行数据
        :param sql:
        :param params:
        :return:
        '''
        result = None
        try:
            self.connect()
            count = self.cur.execute(sql, params)
            if count != 0:
                title, record = [], {}
                for field in self.cur.description:
                    title.append(field[0])
                response = self.cur.fetchone()
                for _t in title:
                    _v = response[title.index(_t)]
                    if isinstance(_v, datetime.date):
                        _v = str(_v)
                    elif isinstance(_v, bytes):
                        _v = int().from_bytes(_v, byteorder='big', signed=True)
                    record[_t] = _v
                result = record

        except Exception as error:
            log().exception(error)
        finally:
            self.close()
        return result

    def fetch_many_record(self, sql, params=None):
        '''
        :param sql:
        :param params:
        :return:
        '''
        try:
            self.connect()
            count = self.cur.execute(sql, params)
            if count != 0:
                title, record = [], {}
                for field in self.cur.description:
                    title.append(field[0])
                response = self.cur.fetchall()
        except Exception as error:
            log().exception(error)
        finally:
            self.close()
        return response, title

    def fetch_all_tables(self, sql, params=None):
        '''
        查询所有的表
        :param sql:
        :param params:
        :return:
        '''
        result = set()
        try:
            self.connect()
            count = self.cur.execute(sql, params)
            if count != 0:
                title, record = [], {}
                for field in self.cur.description:
                    title.append(field[0])
                response = self.cur.fetchall()
                for t in response:
                    result.add(t[0])
        except Exception as error:
            log().exception(error)
        finally:
            self.close()
        return result

    def insert(self, sql, params=None):
        """
        执行新增
        :param sql:
        :param params:
        :return:
        """
        return self.__item(sql, params)

    def __item(self, sql, params=None):
        """
        执行增删改
        :param sql: sql语句
        :param params: sql语句对象的参数列表，默认值为None
        :return: 受影响的行数
        """
        try:
            self.connect()
            count = self.cur.execute(sql, params)
            insert_id = self.cur.lastrowid
            self.conn.commit()
        except Exception:
            raise
        finally:
            self.close()
        return count, insert_id

    def insert_dataset_record(self, dataset_record):
        """插入一条数据包记录"""
        try:
            insert = f"""
                        insert into
                        dataset (name, description, create_user, create_name, 
                        source, create_time, update_time, es_index, quantity) 
                        values 
                            {dataset_record};
                    """
            res, insert_id = self.insert(insert)
        except Exception:
            raise
        return res, insert_id

    def update_dataset_user(self, user_id, dataset_id, status):
        # 本地测试没有任何问题，放到服务器上出现好多问题
        res = None
        try:
            select_sql = f"select id,`status` from user_ref_dataset where " \
                         f"dataset_id = {dataset_id} and user_id ='{user_id}' "
            res = self.fetch_one(select_sql)
            if res is not None:
                # 已经是当前状态了
                id = res['id']
                already_status = res['status']
                if already_status == status:
                    return 1, id
            sql = f"update  user_ref_dataset SET `status`= {status} where " \
                  f"dataset_id = {dataset_id} and user_id ='{user_id}' "
            res = self.__item(sql)
        except Exception as e:
            log().exception(e)
        return res

    def insert_dataset_user(self, dataset_record):
        """插入一条数据包和用户记录"""
        try:
            insert = f"""
                       insert into
                       user_ref_dataset (dataset_id, user_id, status)
                       values
                           {dataset_record};
                           """
            res, insert_id = self.insert(insert)
        except Exception:
            raise
        return res, insert_id

    def get_userId_by_name(self, user_name: str):
        """根据用户名查询用户id"""
        try:
            query = f"""
                       select id from user where username = '{user_name}';
                           """
            if self.fetch_one(query) is None:
                return None
            return self.fetch_one(query)['id']
        except Exception:
            raise

    def get_datasetId(self, already_dataset_id):
        """
        判断已有的数据包是否存在
        :param already_dataset_id: 已有数据包id
        :return:
        """
        try:
            if already_dataset_id is None:
                return None
            query = f"""
                       select id from dataset where id = '{already_dataset_id}';
                           """
            res = self.fetch_one(query)
            if res is not None:
                dataset_id = res['id']
            else:
                return None
        except Exception:
            raise
        return dataset_id

    def delete_dataset_by_ids(self, idList):
        """根据数据包id删除数据包"""
        _del = f"""
                delete from dataset where id in {tuple(idList)}
            """.replace(",)", ")")
        return self.delete(record=_del)

    def update_dataset_record(self, params):
        """
        更新一条数据包记录主要更新数据包中文档的数量
        :param params:
        :return:
        """
        quantity = params["quantity"]
        datasetId = params["datasetId"]
        edge_num = params["edge_num"] if "edge_num" in params else 0
        _update = f"""
                UPDATE dataset SET quantity={quantity},edge_num={edge_num}
                WHERE id = {datasetId}
            """
        res, insert_id = self.update(_update)
        return res, insert_id

    def get_dataset_name_by_dataset_id(self, dataset_id):
        """根据数据包id获取数据包名称"""
        try:
            query = f"""
                    select name from dataset where id ={dataset_id};
                   """
            dataset_name = self.fetch_one(query)["name"]
        except Exception:
            return ""
        return dataset_name

    def update_dataset_index(self, dataset_id, index_name):
        """追加数据包记录的索引"""
        update = f"""
               update
                   dataset
               set
                   `es_index` = '{"{}".format(index_name)}'
               WHERE
                   id = {dataset_id}
           """
        self.__item(update)
        return "ok"

    def update_dataset_source(self, dataset_id, source):
        """
        更新数据包来源
        """
        update = f"""
               update
                   dataset
               set
                   `source` = {source}
               WHERE
                   id = {dataset_id}
           """
        res = self.__item(update)
        return res

    def find_schema_field_and_match(self):
        """获取schema中对应的关系"""
        query = """
            select
                field, `match`
            from
                `schema`
        """
        res = {}

        def get_str_to_list(str_list):
            str_list = str_list.split("[")[1]
            str_list = str_list.split("]")[0]
            str_list = str_list.replace("”", '').replace("“", "").replace("\"", '').replace(" ", '')
            if str_list == "":
                return []
            return str_list.split(",")

        data = self.fetch_many_record(query)
        for tuples in data[0]:
            field = tuples[0]
            match_list = get_str_to_list(tuples[1])
            res[field] = match_list
        return res
