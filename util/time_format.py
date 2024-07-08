# -*- coding: utf-8 -*-
# @Time    : 2021/8/8 15:28
# @Author  : chenbo

import datetime, time


# 生成13位时间戳
# 要求必须是 %Y-%m-%d %H:%M:%S  格式的
def get_time_stamp13(datetime_str):
    # 生成13时间戳   eg:1557842280000
    datetime_obj = datetime.datetime.strptime(datetime_str, '%Y-%m-%d %H:%M:%S')
    # 10位，时间点相当于从1.1开始的当年时间编号
    date_stamp = str(int(time.mktime(datetime_obj.timetuple())))
    # 3位，微秒
    data_microsecond = str("%06d" % datetime_obj.microsecond)[0:3]
    date_stamp = date_stamp + data_microsecond
    return int(date_stamp)


# 生成10位时间戳(精确到毫秒)
# format_str：格式化类型，比如( %Y-%m-%d %H:%M:%S )
def get_time_stamp10(datetime_str, format_str):
    # 生成10时间戳   eg:1614756611
    datetime_obj = datetime.datetime.strptime(datetime_str, format_str)
    # 10位，时间点相当于从1.1开始的当年时间编号
    date_stamp = int(time.mktime(datetime_obj.timetuple()))
    return date_stamp
