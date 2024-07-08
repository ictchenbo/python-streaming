# -*- coding: utf-8 -*-
# @Time    : 2021/8/8 15:28
# @Author  : chenbo

class ModuleManager:
    def __init__(self):
        self.modules = {}

    def load(self, prefix, name):
        mod_name = f"{prefix}.{name}"
        if mod_name not in self.modules:
            try:
                mod = __import__(mod_name, fromlist=(mod_name,))
            except Exception as e:
                print(mod_name, "not found")
                return None
            self.modules[mod_name] = mod

        return self.modules[mod_name].load

    def load_runner(self, mod_name, config):
        """
        支持加载函数或初始化对象 对象必须实现__call__方法
        """
        try:
            mod = __import__(mod_name, fromlist=(mod_name,))
            load = mod.load
            if isinstance(load, type):
                # 是对象类型
                return load(config)
            elif hasattr(load, "__call__"):
                return load
        except Exception as e:
            print(mod_name, "not found")
            raise e

        return None


manager = ModuleManager()
