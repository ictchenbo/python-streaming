# -*- coding: utf-8 -*-
# @Time    : 2021/8/8 15:28
# @Author  : chenbo

import time
import chardet


def load(filepath):
    texts = []
    encoding = chardet.detect(open(filepath, 'rb').read(1000)).get('encoding', 'utf-8')
    with open(filepath, "r", encoding=encoding) as f:
        for item in f:
            texts.append(item)
    return {
        "content": ''.join(texts).replace('\n', '')
    }


def asTime(s):
    return int(time.mktime(time.strptime(s[2:16], '%Y%m%d%H%M%S')))
