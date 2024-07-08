# -*- coding: utf-8 -*-
# @Time    : 2021/8/8 15:28
# @Author  : chenbo


from util.ds_util import *


def load(graph, config):
    """
    基于图谱结构，以内建规则提取空间信息 结果包括地点（Point）、文档-地点、事件地点
    :param graph: 图谱结构
    :param config: 配置信息
    :return:
    """

    groups = group_by(graph.get("nodes", []), "metaType", "entity")

    docs = groups.get("document", [])
    # 获取包含location属性的实体
    locations = filter(lambda ent: ent.get("location"), groups.get("entity", []))

    geo_links = []
    geo_map = {}

    doc = docs[0] if docs else None

    for loc in locations:
        lonlat = loc.get("location")
        if not lonlat:
            continue
        entity_id = loc["id"]
        geo_id = ""
        # 空间对象
        geo_object = {
            "id": geo_id,
            "type": "Point",
            "coordinates": lonlat
        }
        geo_map[loc["name"]] = geo_object

        # 实体-空间对象
        geo_links.append({
            "object": entity_id,
            "metaType": "entity",
            "geoId": geo_id,
            "relationType": loc["type"],
            "name": loc["name"]
        })

        # 文档-空间对象
        if doc:
            geo_links.append({
                "object": doc["id"],
                "metaType": "document",
                "geoId": geo_id,
                "relationType": "report",
                "title": doc.get("title"),
                "site_name": doc.get("site_name"),
                "channel": doc.get("channel", 1),
                "publishTime": doc.get("pt", 0)
            })

    # 事件-空间对象
    events = groups.get("event", [])
    for event in events:
        locs = event.get("location")
        if not locs:
            continue
        for loc in locs:
            loc_name = loc.get("name")
            if loc_name and geo_map.get(loc_name, {}).get("id"):
                geo_links.append({
                    "object": event["id"],
                    "metaType": "event",
                    "geoId": geo_map[loc_name]["id"],
                    "relationType": "locate",
                    "DocId": doc["id"] if doc else "",
                    "type": event.get("type"),
                    "subtype": event.get("subtype"),
                    "publishTime": doc.get("pt") if doc else 0
                })

    graph["geodata"] = {
        "object": [geo_map[k] for k in geo_map.keys()],
        "links": geo_links
    }
    return graph
