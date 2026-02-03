"""Aptem tap class."""

from __future__ import annotations

import requests
from singer_sdk import Tap
from singer_sdk import typing as th
from typing_extensions import override

from tap_aptem import metadata
from tap_aptem.client import AptemODataStream


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

        streams = []

        for e in metadata.discover_entities(response.text):
            stream = AptemODataStream(
                tap=self,
                name=e.name,
                schema=e.jsonschema,
                path=f"/{e.name}",
            )
            stream.primary_keys = e.primary_keys
            stream.replication_key = e.replication_key

            streams.append(stream)

        return streams


if __name__ == "__main__":
    TapAptem.cli()
