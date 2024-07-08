# -*- coding: utf-8 -*-
# @Time    : 2021/8/8 15:28
# @Author  : chenbo

from node_base import BaseNode
from module_manager import manager


class load(BaseNode):
    def __init__(self, config):
        print("init writer.run:load")
        BaseNode.__init__(self, config)
        targets = config.get("targets", [])
        if not isinstance(targets, list):  # 支持写单个或数组
            targets = [targets]
        self.runners = {}
        self.inner_configs = {}
        for target in targets:
            comp_config = config.get(target, {})
            comp_config["_task"] = config["_task"]
            runner = manager.load_runner(f"writer.write_{target}", comp_config)
            if runner:
                self.runners[target] = runner
                self.inner_configs[target] = comp_config

    def load(self, data):
        for target, runner in self.runners.items():
            runner(data, self.inner_configs.get(target))
        return data

    def close(self):
        for runner in self.runners:
            if hasattr(runner, "close"):
                runner.close()
