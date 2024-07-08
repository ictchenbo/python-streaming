# -*- coding: utf-8 -*-
# @Time    : 2021/8/8 15:28
# @Author  : chenbo

def load(doc, config):
    """
    基于文档结构 转换成子图结构，包括文档、实体、事件、文档-实体、文档-事件、事件-实体
    :param doc: 文档（经过NLU处理的）
    :param config: 配置信息
    :return:
    """
    parser_config = config.get("_task").get("parser")
    parser_name = parser_config.get("name", "corenlu")  # 文档解析器名称
    parser_field = config.get("parser_field", "content")  # 解析的字段

    # 是否保留解析结果字段
    keep_parse_field = config.get("keep_parse_field", False)
    # 提升字段
    shift_fields = config.get("shift_fields", ["topic", "sentiment", "keywords"])

    field_origin = f"_{parser_name}_{parser_field}"

    field_ner = f"_ner_"  # 实体识别结果
    field_nel = f"_nel_"  # 实体链接结果
    field_event = f"_event_"  # 事件抽取结果

    links = []

    doc_id = doc.pop("_id")

    doc_node = dict(**doc)
    doc_node["id"] = doc_id
    doc_node["metaType"] = "document"

    # 字段提升
    for f in shift_fields:
        old_f = f"_{f}_"
        if old_f in doc_node:
            doc_node[f] = doc_node[old_f]
            if not keep_parse_field:
                del doc_node[old_f]

    ent_map = {ent["name"]: ent for ent in doc.get(field_ner, [])}  # 实体映射表
    entities = [ent_map[k] for k in ent_map.keys()]  # 非重复实体列表

    # 根据实体链接结果，附加实体id
    for nel_item in doc.get(field_nel, []):
        name = nel_item["name"]
        if name in ent_map:
            ent_map[name]["id"] = nel_item["id"]
            if "location" in nel_item and nel_item["location"]:
                ent_map[name]["location"] = nel_item["location"]

    # 文档-实体关系
    for ent in entities:
        if "id" not in ent:
            ent["id"] = doc_id + "_" + ent["name"]
        links.append({
            "fromId": doc_id,
            "fromType": "document",
            "toId": ent["id"],
            "toType": "entity",
            "type": "包含"
        })

    events = doc.get(field_event, [])
    event_num = 0
    # 事件信息：事件、文档-事件、事件-实体
    for event_node in events:
        event_node["metaType"] = "event"
        # 生成事件ID
        if "id" not in event_node:
            event_node["id"] = doc_id + "_" + str(event_num)
        event_num += 1

        # 文档-事件
        links.append({
            "fromId": doc_id,
            "fromType": "document",
            "toId": event_node["id"],
            "toType": "event",
            "type": "包含"
        })

        # 事件-实体
        for event_ent in event_node.get("entity", []):
            ent_name = event_ent["name"]
            if ent_map.get(ent_name, {}) and ent_map[ent_name].get("id"):
                links.append({
                    "fromId": event_node["id"],
                    "fromType": "event",
                    "toId": ent_map[ent_name]["id"],
                    "toType": "entity",
                    "type": "包含"
                })

    # 删除无用字段
    if not keep_parse_field:
        for f in [field_ner, field_nel, field_event, field_origin]:
            if f in doc_node:
                del doc_node[f]

    nodes = [doc_node]
    nodes.extend(entities)
    nodes.extend(events)

    return {
        "nodes": nodes,
        "edges": links
    }
