# -*- coding: utf-8 -*-
# @Time    : 2021/8/8 15:28
# @Author  : chenbo

import time
import traceback
from multiprocessing import Process
from threading import Thread

from module_manager import manager

STOP_KEY = "__stop__"
STOP_ROW = {"_id": STOP_KEY, "stop_signal": 1}


def process(process, read_table, write_table, fun, config, mq):
    batch = config.get("batch", 1)
    while True:
        mongo_data = mq.mq_reader(read_table, limit=batch)
        if not mongo_data:
            time.sleep(5)
            continue
        if isinstance(mongo_data, list) and len(mongo_data) == 1:
            mongo_data = mongo_data[0]
        if isinstance(mongo_data, dict) and mongo_data.get("_id") == STOP_KEY:
            mq.mq_delete(read_table, STOP_KEY)
            mq.mq_writer(write_table, STOP_ROW)
            break
        # print(process, mongo_data)
        try:
            data = fun(mongo_data, config)
            # 允许不做输出
            if write_table and data:
                if isinstance(data, dict):
                    mq.mq_writer(write_table, data)
                if isinstance(data, list):
                    for row in data:
                        # if not row.get("_id"):
                        #     row["_id"] = mongo_data["_id"]
                        mq.mq_writer(write_table, row)
        except Exception as e:
            traceback.print_exc()
            mongo_data["error"] = "{}".format(e)
            mq.mq_writer("{}_error".format(process), mongo_data)
        # 完成后 移除数据
        if isinstance(mongo_data, list):
            for item in mongo_data:
                mq.mq_delete(read_table, item)
        else:
            mq.mq_delete(read_table, mongo_data.get("_id"))


class ProcessRunner(object):
    def __init__(self, name, config, _input, _output, mq=None):
        self.name = name  # 环节名称
        self.config = config  # 环节配置
        self.input = _input  # 输入队列，为空表示自己能够产生输入，而不是从输入队列中获取
        self.output = _output  # 输出队列，为空表示忽略产生的输出
        mod_name = config.get("_module") or f'{name}.run'  # 模块名，默认为<name>.run
        self.fun = manager.load_runner(mod_name, config)  # 加载组件
        assert self.fun is not None
        self.mq = mq

    def run(self):
        print("starting {}".format(self.name))
        mq = self.mq
        if not self.input:  # 无需输入队列
            self.fun(self.config, lambda doc: mq.mq_writer(self.output, doc))
            mq.mq_writer(self.output, STOP_ROW)
        else:
            process(self.name, self.input, self.output, self.fun, self.config, mq)
        print("ending {}".format(self.name))

    def close(self):
        if hasattr(self.fun, "close"):
            self.fun.close()


def start_process(mq_server, task_config):
    """
    启动处理流程
    :param mq_server 消息队列
    :param task_config 任务配置
    :return:
    """
    process_list = task_config["process"]
    engine = task_config.get("engine", "thread")
    pipes = []
    p_list = []
    # 开始的队列，支持从非loader环节开始
    _input = task_config.get("start")
    # 启动处理流程
    for process_item in process_list:
        config = task_config.get(process_item, {})
        config["_task"] = task_config
        _output = process_item  # 默认输出队列
        if "_output" in config:
            _output = config.get("_output")
        pipe = ProcessRunner(process_item, config, _input, _output, mq_server)
        pipes.append(pipe)
        p = Thread(target=pipe.run) if engine == "thread" else Process(target=pipe.run)

        p.start()
        p_list.append(p)
        _input = _output

    return pipes, p_list


def wait_for(p_list):
    """
    等待进程结束
    :param p_list:
    :return:
    """
    for p_item in p_list:
        p_item.join()


def init_mq(mq_config, engine=None):
    """
    初始化消息队列
    :param p_list:
    :return:
    """
    mq_type = mq_config.get("type", "local")
    server_config = mq_config.get(mq_type, {})
    if mq_type == "mongo":
        from mq.mq_mongo import MqMongo
        return MqMongo(server_config)
    else:
        from mq.mq_local import MqLocal, MqQueue
        if engine == "process":
            return MqLocal(server_config)
        else:
            return MqQueue(server_config)


if __name__ == "__main__":
    import argparse
    import json
    import os
    import sys
    from conf import mq_config

    arg_parser = argparse.ArgumentParser('GoIN-Streaming 离线数据解析处理系统')
    arg_parser.add_argument('--mq', type=str, default='local', help='MQ类型，默认本地消息队列')
    arg_parser.add_argument('--engine', type=str, help='执行引擎，可选thread/process')
    arg_parser.add_argument('--task', type=str, default='task.json', help='任务文件')
    arg_parser.add_argument('--start', type=str, help='指定开始环节，用于支持非loader开始的处理')
    arg_parser.add_argument('--process', type=str, help='指定流程环节')
    arg_parser.add_argument('--source', type=str, help='来源文件')
    arg_parser.add_argument('--target', type=str, help='目标环节')
    arg_parser.add_argument('--output', type=str, help='输出文件')

    args = arg_parser.parse_args()
    task_file = args.task

    if not os.path.exists(task_file):
        print("task-file not found")
        sys.exit(1)

    if args.mq:
        mq_config['type'] = args.mq

    with open(task_file, "r", encoding="UTF-8") as f:
        task_config = json.load(f)

    # 指定引擎
    if args.engine:
        task_config["engine"] = args.engine

    mq_server = init_mq(mq_config, task_config.get("engine"))

    # 指定开始环节
    if args.start:
        task_config["start"] = args.start
    # 支持指定流程环节
    if args.process:
        task_config["process"] = args.process.split(",")
    # 支持指定来源文件
    if args.source and "loader" in task_config:
        task_config["loader"]["source"] = {
            "type": "file",
            "file": {
                "path": args.source
            }
        }
    # 支持指定末端目标
    if args.target and "writer" in task_config:
        task_config["writer"]["targets"] = args.target.split(",")

    if args.output and "writer" in task_config:
        task_config["writer"]["targets"] = ["file"]
        task_config["writer"]["file"] = {
            "format": "jsonl",
            "path": args.output,
            "open_mode": "append"
        }

    pipes, plist = start_process(mq_server, task_config)
    wait_for(plist)
    for pipe in pipes:
        pipe.close()
