# -*- coding: utf-8 -*-
# @Time    : 2021/8/8 15:28
# @Author  : chenbo

from conf import PORT
from rest_app_base import app

if __name__ == "__main__":
    import web.main_parse_writer

    app.run(host='0.0.0.0', port=PORT, debug=True)
