# -*- coding: utf-8 -*-
# @Time    : 2021/8/8 15:28
# @Author  : chenbo

import datetime
import json

from flask import make_response


class DateTimeEncoder(json.JSONEncoder):
    def default(self, z):
        if isinstance(z, datetime.datetime) or isinstance(z, datetime.date):
            return str(z)
        else:
            return super().default(z)


def asResponse(ret):
    body = json.dumps(ret, ensure_ascii=False, cls=DateTimeEncoder)

    resp = make_response(body, str(ret["status"] // 100))

    resp.headers['Content-Type'] = 'application/json; charset=utf-8'

    return resp


def asJson(data=None, pager=None, status=20000, msg=None):
    ret = {"status": status}
    if msg:
        ret["msg"] = msg
    if data:
        ret["data"] = data
    if pager:
        ret["pager"] = pager

    return asResponse(ret)
