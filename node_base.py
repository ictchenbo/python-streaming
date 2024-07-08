
class BaseNode(object):
    def __init__(self, config):
        self.config = config

    def load(self, data):
        return data

    def close(self):
        pass

    def __call__(self, data, config=None):
        return self.load(data)
