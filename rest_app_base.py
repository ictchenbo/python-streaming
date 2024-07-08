# -*- coding: utf-8 -*-
# @Time    : 2021/8/8 15:28
# @Author  : chenbo

from flask import Flask
from conf import APP_NAME

app = Flask(APP_NAME)
app.config['SWAGGER'] = {
    'title': '结构化和非结构化后台批量入库 API',
    'author': '王明明',
    'description': '提供结构化和非结构化数据入库',
    'version': '1.0'
}
