# -*- coding: utf-8 -*-
# @Time    : 2021/8/8 15:28
# @Author  : chenbo

import logging
import traceback
from functools import wraps
import json
import datetime
from bson import ObjectId
from flask import request, g, make_response
from pymongo.results import InsertManyResult

from conf import LOGGER_LEVEL

logger = logging.getLogger()
if LOGGER_LEVEL.lower() == "error":
    logger.setLevel(logging.ERROR)
elif LOGGER_LEVEL.lower() == "warning":
    logger.setLevel(logging.WARNING)
elif LOGGER_LEVEL.lower() == "info":
    logger.setLevel(logging.INFO)

logging.basicConfig(
    format='%(asctime)s,%(msecs)d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s',
    datefmt='%d-%m-%Y:%H:%M:%S')


def log(func):
    def wrapper(*args, **kw):
        logger.warning("")
        return func(*args, **kw)

    return wrapper


def pre_validate(func):
    """User property initialization processing 检查用户信息 应作为第一个前置包装器"""

    @wraps(func)
    def decorator(*args, **kwargs):
        userid = request.headers.get("userid")
        if userid is None:
            return {"code": 40001,
                    "message": "please input userid in header"}, 400
        return func(*args, **kwargs)

    return decorator


def return_state(label):
    """Generates the interface's return state and information 应作为最后一个后置包装器"""

    def actual_decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                g.ts = int(request.args.get("ts", "0"))
                result = func(*args, **kwargs)
                if isinstance(result, dict) and "code" in result:
                    return asResponse(result,
                                      status=result["code"])  # 返回字典结构 原样返回
                else:
                    return asJson(data=result)  # 作为data返回
            except AttributeError:
                raise
                return asJson(code=40003,
                              msg="Input parameter error, please verify")
            except Exception as e:
                logger.error(
                    f"""
                    ############### error-help! ###################
                        error detail:{traceback.format_exc()}
                    ###############################################
                    """
                )
                return asJson(code=50001,
                              msg="Internal service exception: {}".format(e))

        return wrapper

    return actual_decorator


class DateTimeEncoder(json.JSONEncoder):
    def default(self, z):
        if isinstance(z, datetime.datetime) or isinstance(z, datetime.date):
            return (str(z))
        elif isinstance(z, InsertManyResult):
            return str(z, encoding='utf-8')
        elif isinstance(z, ObjectId):
            return str(z)
        else:
            return super().default(z)


def asResponse(ret, status=20000):
    ret["ts"] = g.ts
    body = json.dumps(ret, ensure_ascii=False, cls=DateTimeEncoder)

    if status < 20000:
        status = status * 100

    resp = make_response(body, str(status // 100))

    resp.headers['Content-Type'] = 'application/json; charset=utf-8'
    return resp


def asJson(data=None, code=20000, msg=None):
    ret = {"code": code}
    if code != 20000:
        ret["message"] = msg
    else:
        ret["data"] = data

    return asResponse(ret, code)
