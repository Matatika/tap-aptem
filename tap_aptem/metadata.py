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
class ComplexType:
    name: str
    properties: dict[str, str]
    open_type: bool


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


def _iter_schema_elements(root: ElementTree):
    for element in root.iter():
        if _local_name(element.tag) == "Schema":
            yield element, element.attrib["Namespace"]


def _extract_properties(type_element: Element):
    return {
        prop.attrib["Name"]: prop.attrib["Type"]
        for prop in _iter_children_by_name(type_element, "Property")
    }


def _extract_complex_types(root: ElementTree):
    complex_types: dict[str, ComplexType] = {}

    for schema, namespace in _iter_schema_elements(root):
        for complex_type in _iter_children_by_name(schema, "ComplexType"):
            name = complex_type.attrib["Name"]
            type_id = f"{namespace}.{name}"

            complex_types[type_id] = ComplexType(
                name,
                _extract_properties(complex_type),
                complex_type.attrib.get("OpenType") == "true",
            )

    return complex_types


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

    for schema, namespace in _iter_schema_elements(root):
        for entity_type in _iter_children_by_name(schema, "EntityType"):
            entity_type_name = entity_type.attrib["Name"]
            entity_id = f"{namespace}.{entity_type_name}"

            keys = [
                ref.attrib["Name"]
                for key in _iter_children_by_name(entity_type, "Key")
                for ref in _iter_children_by_name(key, "PropertyRef")
            ]

            entities_by_type[entity_id] = EntityInfo(
                name=entity_type_name,
                properties=_extract_properties(entity_type),
                keys=tuple(keys),
            )

    return entities_by_type


def _type_to_jsonschema(prop_type: str, complex_types: dict[str, ComplexType]):
    if prop_type.startswith("Collection("):
        wrapped_type = prop_type.removeprefix("Collection(").removesuffix(")")
        return th.ArrayType(_type_to_jsonschema(wrapped_type, complex_types))

    if complex_type := complex_types.get(prop_type):
        return th.ObjectType(
            *_properties_to_jsonschema(complex_type.properties, complex_types),
            additional_properties=complex_type.open_type,
        )

    return EDM_TYPE_MAP.get(prop_type, th.StringType)


def _properties_to_jsonschema(
    properties: dict[str, str],
    complex_types: dict[str, ComplexType],
):
    yield from (
        th.Property(name, _type_to_jsonschema(type_, complex_types))
        for name, type_ in properties.items()
    )


def discover_entities(xml: str):
    root = defusedxml.ElementTree.fromstring(xml)

    complex_types = _extract_complex_types(root)
    entity_sets_by_type = _extract_entity_sets_by_type(root)
    entities_by_type = _extract_entities_by_type(root)

    for entity_type_name, entity_set_name in entity_sets_by_type.items():
        entity = entities_by_type[entity_type_name]

        properties = list(_properties_to_jsonschema(entity.properties, complex_types))

        yield DiscoveredEntity(
            name=entity_set_name,
            jsonschema=th.PropertiesList(*properties).to_dict(),
            primary_keys=entity.keys,
            replication_key=next(
                (p.name for p in properties if p.name == "UpdatedDate"),
                None,
            ),
        )
