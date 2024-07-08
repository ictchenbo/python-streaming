# -*- coding: utf-8 -*-
# @Time    : 2021/8/8 15:28
# @Author  : chenbo


import sys

from readfile_framework import for_json_line, print_json


def get_field_value(field):
    def get_value(row):
        return row.get(field)

    return get_value


def parse_field(field):
    if isinstance(field, str) and field.startswith('@'):
        return get_field_value(field[1:])
    return lambda _: field


def parse_rules(rules):
    rule_map = {}
    for rule in rules:
        target = rule
        source = None
        if ":" in rule:
            pos = rule.find(":")
            target = rule[:pos]
            source = rule[pos + 1]
        source = source or ("@" + target)
        rule_map[target] = parse_field(source)

    return rule_map


class JsonTransform:
    def __init__(self, rules):
        if isinstance(rules, str):
            self.rule_map = parse_rules(rules.split(","))
        elif isinstance(rules, list):
            self.rule_map = parse_rules(rules)
        elif isinstance(rules, dict):
            self.rule_map = {}
            for target, s in rules.items():
                self.rule_map[target] = parse_field(s)

    def row_map(self, row, _):
        ret = {}
        for t, sFun in self.rule_map.items():
            res = sFun(row)
            if res is not None:
                ret[t] = res
        return ret

    def process(self, file, map_func=None, after_func=None):
        map_func = map_func or (lambda row, _: self.row_map(row, _))
        after_func = after_func or print_json

        def callback(row, _):
            ret = map_func(row, _)
            after_func(ret, _)

        for_json_line(file, callback)


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("args need! <input> <rules>")
        sys.exit(1)

    pipe = JsonTransform(sys.argv[2])
    pipe.process(sys.argv[1])
