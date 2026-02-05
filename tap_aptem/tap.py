"""Aptem tap class."""

from __future__ import annotations

import requests
from singer_sdk import Tap
from singer_sdk import typing as th
from typing_extensions import override

from tap_aptem import metadata
from tap_aptem.client import AptemODataStream, EmbeddedCollectionStream


class TapAptem(Tap):
    """Singer tap for the Aptem OData API."""

    name = "tap-aptem"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "api_token",
            th.StringType,
            required=True,
            description="API token for the Aptem OData API.",
        ),
        th.Property(
            "tenant_name",
            th.StringType,
            required=True,
            description="Aptem tenant name used to build the base URL.",
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
            description="Start date for incremental replication.",
        ),
    ).to_dict()

    @override
    def discover_streams(self):
        tenant_name = self.config["tenant_name"]
        url = f"https://{tenant_name}.aptem.co.uk/odata/1.0/$metadata"

        response = requests.get(url, timeout=300)
        response.raise_for_status()

        streams_by_entity_name: dict[str,] = {}

        for entity in metadata.discover_entities(response.text):
            if parent_stream := streams_by_entity_name.get(entity.parent_entity_name):
                stream_cls = type(
                    f"{entity.collection_name}EmbeddedStream",
                    (EmbeddedCollectionStream,),
                    {
                        "parent_stream_type": type(parent_stream),
                        "parent_entity_name": entity.parent_entity_name,
                        "collection_name": entity.collection_name,
                    },
                )

            else:
                stream_cls = type(
                    f"{entity.name}AptemODataStream",
                    (AptemODataStream,),
                    {
                        "entity_name": entity.name,
                        "path": f"/{entity.collection_name}",
                    },
                )

            stream = stream_cls(
                tap=self,
                name=entity.collection_name,
                schema=entity.jsonschema,
            )

            stream.primary_keys = entity.primary_keys
            stream.replication_key = entity.replication_key

            streams_by_entity_name[entity.name] = stream

        return streams_by_entity_name.values()


if __name__ == "__main__":
    TapAptem.cli()
