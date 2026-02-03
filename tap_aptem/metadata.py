"""OData $metadata parsing helpers."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

import defusedxml.ElementTree
from singer_sdk import typing as th

if TYPE_CHECKING:
    from xml.etree.ElementTree import Element, ElementTree

EDM_TYPE_MAP = {
    "Edm.String": th.StringType,
    "Edm.Boolean": th.BooleanType,
    "Edm.Int16": th.IntegerType,
    "Edm.Int32": th.IntegerType,
    "Edm.Int64": th.IntegerType,
    "Edm.Byte": th.IntegerType,
    "Edm.SByte": th.IntegerType,
    "Edm.Decimal": th.NumberType,
    "Edm.Double": th.NumberType,
    "Edm.Single": th.NumberType,
    "Edm.DateTime": th.DateTimeType,
    "Edm.DateTimeOffset": th.DateTimeType,
    "Edm.Date": th.DateType,
    "Edm.TimeOfDay": th.TimeType,
    "Edm.Guid": th.UUIDType,
    "Edm.Binary": th.StringType,
}


@dataclass(frozen=True)
class EntityInfo:
    name: str
    properties: dict[str, str]
    keys: tuple[str, ...]


@dataclass(frozen=True)
class DiscoveredEntity:
    name: str
    jsonschema: dict
    primary_keys: tuple[str, ...]
    replication_key: str


def _local_name(tag: str):
    return tag.split("}", 1)[-1]


def _iter_children_by_name(element: Element, name: str):
    for child in element:
        if _local_name(child.tag) == name:
            yield child


def _extract_entity_sets_by_type(root: Element):
    entity_sets_by_type: dict[str, str] = {}

    for element in root.iter():
        if _local_name(element.tag) != "EntityContainer":
            continue

        for entity_set in _iter_children_by_name(element, "EntitySet"):
            entity_type = entity_set.attrib["EntityType"]
            entity_sets_by_type[entity_type] = entity_set.attrib["Name"]

    return entity_sets_by_type


def _extract_entities_by_type(root: ElementTree):
    entities_by_type: dict[str, EntityInfo] = {}

    for element in root.iter():
        if _local_name(element.tag) != "Schema":
            continue

        namespace = element.attrib["Namespace"]

        for entity_type in _iter_children_by_name(element, "EntityType"):
            entity_type_name = entity_type.attrib["Name"]
            entity_id = f"{namespace}.{entity_type_name}"

            properties = {
                prop.attrib["Name"]: prop.attrib["Type"]
                for prop in _iter_children_by_name(entity_type, "Property")
            }

            keys = [
                ref.attrib["Name"]
                for key in _iter_children_by_name(entity_type, "Key")
                for ref in _iter_children_by_name(key, "PropertyRef")
            ]

            entities_by_type[entity_id] = EntityInfo(
                name=entity_type_name,
                properties=properties,
                keys=tuple(keys),
            )

    return entities_by_type


def _prop_type_to_jsonschema(prop_type: str):
    if prop_type.startswith("Collection("):
        wrapped_type = prop_type.removeprefix("Collection(").removesuffix(")")
        return th.ArrayType(_prop_type_to_jsonschema(wrapped_type))

    return EDM_TYPE_MAP.get(prop_type, th.StringType)


def discover_entities(xml: str):
    root = defusedxml.ElementTree.fromstring(xml)

    entity_sets_by_type = _extract_entity_sets_by_type(root)
    entities_by_type = _extract_entities_by_type(root)

    for entity_type_name, entity_set_name in entity_sets_by_type.items():
        entity = entities_by_type[entity_type_name]

        properties = [
            th.Property(
                prop_name,
                _prop_type_to_jsonschema(prop_type),
            )
            for prop_name, prop_type in entity.properties.items()
            if "_" not in prop_name
        ]

        yield DiscoveredEntity(
            name=entity_set_name,
            jsonschema=th.PropertiesList(*properties).to_dict(),
            primary_keys=entity.keys,
            replication_key=next(
                (p.name for p in properties if p.name == "UpdatedDate"),
                None,
            ),
        )
