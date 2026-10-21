# Copyright (c) 2026 Meltano.

"""Tests for the Aptem OData stream class."""

import io
import json
from urllib.parse import parse_qs, urlencode, urlparse

import requests
from requests.adapters import BaseAdapter

from tap_aptem.client import AptemODataV2Stream
from tap_aptem.tap import TapAptem

RECORD_COUNT = 10_500
MAX_PAGE_SIZE = 1000


class ODataV2Adapter(BaseAdapter):
    """Serve records in pages of up to 1,000 items, as the OData v2 feed does."""

    def send(self, request, **kwargs):  # noqa: ARG002, D102
        url = urlparse(request.url)
        query = {k: v[0] for k, v in parse_qs(url.query).items()}
        skip = int(query.get("$skip", 0))
        end = min(skip + int(query.get("$top", RECORD_COUNT)), RECORD_COUNT)
        start = int(query.get("$skiptoken", skip))
        page_end = min(start + MAX_PAGE_SIZE, end)

        body = {"value": [{"Id": i} for i in range(start, page_end)]}

        if page_end < end:
            query["$skiptoken"] = page_end
            body["@odata.nextLink"] = url._replace(query=urlencode(query)).geturl()

        response = requests.Response()
        response.status_code = 200
        response.raw = io.BytesIO(json.dumps(body).encode())
        response.request = request
        return response

    def close(self):  # noqa: D102
        pass


def test_odata_v2_full_table_sync_gets_every_record():
    """Follow `@odata.nextLink` on OData v2, rather than skip by the page size."""
    tap = TapAptem(
        config={"tenant_name": "tenant", "api_token": "token", "odata_version": "2.0"},
        setup_mapper=False,
    )
    stream = AptemODataV2Stream(
        tap,
        name="Actions",
        schema={"properties": {"Id": {"type": "integer"}}},
        path="/Actions",
    )
    stream.requests_session.mount("https://", ODataV2Adapter())

    ids = [record["Id"] for record in stream.get_records(context=None)]

    assert ids == list(range(RECORD_COUNT))  # noqa: S101
