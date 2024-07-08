# -*- coding: utf-8 -*-
# @Time    : 2021/8/8 15:28
# @Author  : chenbo

import json
from util.ds_util import *

location_map = json.load(open("mapper/goin/location_map.json"))
news_media_map = json.load(open("mapper/goin/news_media.json"))


def select(arr, key):
    return [item.get(key) for item in arr]


def load(graph, config):
    """
    GoIN的图结构 添加其他GoIN字段 包括文档
    :param graph:
    :param config:
    :return:
    """
    parser_config = config.get("_task").get("parser")
    parser_name = parser_config.get("name", "corenlu")  # 文档解析器名称
    parser_field = config.get("parser_field", "content")  # 解析的字段

    field_origin = f"_{parser_name}_{parser_field}"

    groups = group_by(graph.get("nodes", []), "metaType", "entity")
    docs = groups.get("document", [])
    entity = groups.get("entity", [])

    for doc in docs:
        if field_origin not in doc:
            continue

        if "channel" not in doc:
            doc["channel"] = 1
        pt = int(doc.get("pt") or doc.get("publish_time") or "0")
        doc["time"] = pt

        nlu_result = doc.get(field_origin, {})

        props = []

        def add(k, v):
            if k and v:
                props.append({"name": k, "value": v})

        add("url", doc.get("url"))
        add("topic", doc.get("topic", "").lower())

        # 属地
        if 'originate' in doc or 'locate' in doc:
            originate = doc.get('originate') or doc.get('locate')
            if originate in location_map:
                originate_info = location_map[originate]
                add('location_origin.id', originate_info['id'])
                add('location_origin.name', originate)
                add('location_origin.longitude', originate_info['longitude'])
                add('location_origin.latitude', originate_info['latitude'])

        if "location_locate" in doc:
            item = doc["location_locate"]
            id = item["id"] if "id" in item else ""
            name = item["name"] if "name" in item else ""
            if id == "":
                longitude = 0
                latitude = 0
            else:
                longitude = item["geometry"]["coordinates"][0]
                latitude = item["geometry"]["coordinates"][1]
            add('location_origin.id', id)
            add('location_origin.name', name)
            add('location_origin.longitude', longitude)
            add('location_origin.latitude', latitude)

        # entity_list 信息抽取
        add("entity_list.ename", select(entity, "name"))
        add("entity_list.cname", select(entity, 'chinese_name'))
        add("entity_list.id", select(entity, 'id'))
        add("entity_list.type", select(entity, 'entity_type'))

        # 文档数字类型属性
        add("stats_attr.key", [])
        add("stats_attr.value", [])
        add("stats_attr.vtype", [])

        # 句子级情感
        if "sentiment" in nlu_result:
            sentiment = nlu_result["sentiment"]
            values = sentiment.values()
            add('sentences_sentiment.object', [v[0] for v in values])
            add('sentences_sentiment.polarity', [v[1] for v in values])

        # 句子级词
        if "chunk" in nlu_result:
            chunk = nlu_result["chunk"]
            content, label, sidx, widx = [], [], [], []
            for sent_id, sent in chunk.items():
                sent_id = int(sent_id) + 1
                for inf in sent:
                    content.append(inf["chunk"])
                    label.append(inf["label"])
                    sidx.append(sent_id)
                    widx.append(inf["start"])

            add('sentences_words.content', content)
            add('sentences_words.label', label)
            add('sentences_words.sidx', sidx)
            add('sentences_words.widx', widx)

        # 句子级言论
        if "opinion" in nlu_result:
            text, object, time, sidx = [], [], [], []
            opinion = nlu_result["opinion"]
            for sent_id, sent in opinion.items():
                sent_id = int(sent_id) + 1
                for inf in sent:
                    text.append(inf['content'])
                    object.append(inf['person'])
                    time.append(pt)
                    sidx.append(sent_id)
            add('sentences_speech.text', text)
            add('sentences_speech.object', object)
            add('sentences_speech.time', object)
            add('sentences_speech.sidx', object)

        doc["props"] = props

    ent_name_map = {ent["name"]: ent for ent in entity}

    def get_id(name, key="id"):
        if name and name in ent_name_map:
            return ent_name_map[name].get(key)
        return None

    events = groups.get("events", [])
    for event in events:
        props = []

        def add(k, v):
            if k and v:
                props.append({"name": k, "value": v})

        if "argument" in event:
            arguments = event.pop("argument")
            add("arguments.role", [a.get("role") for a in arguments])
            add("arguments.name", [a.get("name") for a in arguments])
            add("arguments.role", [a.get("type") for a in arguments])
            add("arguments.id", [get_id(a.get("name")) for a in arguments])

        if "entity" in event:
            entity = event.pop("entity")
            add("entity_list.role", [a.get("role") for a in entity])
            add("entity_list.cname", [a.get("name") for a in entity])
            add("entity_list.id", [get_id(a.get("name")) for a in entity])
            add("entity_list.type", [get_id(a.get("name"), "type") for a in entity])

        if "location" in event:
            entity = event.pop("location")
            add("locations.name", [a.get("name") for a in entity])
            add("locations.id", [get_id(a.get("name")) for a in entity])
            add("locations.type", [get_id(a.get("name"), "type") for a in entity])
            add("locations.geo_type", [get_id(a.get("name"), "type") for a in entity])
            add("locations.geo_longitude", [a.get("") for a in entity])
            add("locations.geo_latitude", [get_id(a.get("name"), "type") for a in entity])

    return graph
