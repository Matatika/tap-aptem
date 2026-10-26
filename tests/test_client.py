# Copyright (c) 2026 Meltano.

"""Tests for the Aptem OData stream class."""

import io
import json
from http import HTTPStatus
from urllib.parse import parse_qs, urlencode, urlparse

import pytest
import requests
from requests.adapters import BaseAdapter
from singer_sdk.exceptions import RetriableAPIError

from tap_aptem.client import (
    AptemODataStream,
    AptemODataV2Stream,
    EmbeddedCollectionStream,
)
from tap_aptem.tap import TapAptem

RECORD_COUNT = 10_500
MAX_PAGE_SIZE = 1000


@pytest.fixture
def stream():
    """Return a stream that makes no request on creation."""
    tap = TapAptem(
        config={"tenant_name": "tenant", "api_token": "token"},
        setup_mapper=False,
    )
    return AptemODataStream(tap, name="Actions", schema={"properties": {}})


def _retriable_api_error(status_code: int, headers: dict[str, str]):
    response = requests.Response()
    response.status_code = status_code
    response.headers.update(headers)
    return RetriableAPIError("error", response)


@pytest.mark.parametrize(
    ("status_code", "headers", "expected_waits"),
    [
        pytest.param(
            HTTPStatus.TOO_MANY_REQUESTS,
            {"Retry-After": "30"},
            [30, 30],
            id="rate limited",
        ),
        pytest.param(
            HTTPStatus.TOO_MANY_REQUESTS,
            {},
            [10, 20],
            id="rate limited without Retry-After",
        ),
        pytest.param(
            HTTPStatus.SERVICE_UNAVAILABLE,
            {"Retry-After": "30"},
            [10, 20],
            id="server error",
        ),
    ],
)
def test_backoff_wait(stream, status_code, headers, expected_waits):
    """Wait for the `Retry-After` of a 429, and back off exponentially otherwise."""
    wait_gen = stream.backoff_wait_generator()
    wait_gen.send(None)

    error = _retriable_api_error(status_code, headers)

    assert [wait_gen.send(error) for _ in expected_waits] == expected_waits  # noqa: S101


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


@pytest.mark.parametrize(
    "replication_key",
    [
        pytest.param(None, id="full table"),
        pytest.param("Id", id="incremental"),
    ],
)
def test_odata_v2_sync_gets_every_record(replication_key):
    """Follow `@odata.nextLink` on OData v2, rather than page by the tap's own query."""
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
    stream.replication_key = replication_key
    stream.requests_session.mount("https://", ODataV2Adapter())

    ids = [record["Id"] for record in stream.get_records(context=None)]

    assert ids == list(range(RECORD_COUNT))  # noqa: S101


class ODataV1Adapter(BaseAdapter):
    """Serve records by the `$filter`, `$orderby` and `$top` of each request.

    Aptem times out on a deep `$skip`, so refuse any `$skip`.
    """

    def __init__(self, records) -> None:  # noqa: D107
        super().__init__()
        self.records = records

    def send(self, request, **kwargs):  # noqa: ARG002, D102
        query = {k: v[0] for k, v in parse_qs(urlparse(request.url).query).items()}
        response = requests.Response()
        response.request = request

        if "$skip" in query:
            response.status_code = HTTPStatus.BAD_REQUEST
            body = {"error": {"message": "$skip is not supported"}}
        else:
            records = self.records

            if "$filter" in query:
                condition = (
                    query["$filter"].replace(" eq ", " == ").replace(" gt ", " > ")
                )
                records = [r for r in records if eval(condition, {}, r)]  # noqa: S307

            if "$orderby" in query:
                keys = query["$orderby"].split(",")
                records = sorted(records, key=lambda r: [r[k] for k in keys])

            response.status_code = HTTPStatus.OK
            body = {"value": records[: int(query["$top"])]}

        response.raw = io.BytesIO(json.dumps(body).encode())
        return response

    def close(self):  # noqa: D102
        pass


@pytest.mark.parametrize(
    ("primary_keys", "records"),
    [
        pytest.param(
            ["Id"],
            [{"Id": i} for i in range(RECORD_COUNT)],
            id="single key",
        ),
        pytest.param(
            ["LearnerId", "Order"],
            [{"LearnerId": i // 3, "Order": i % 3} for i in range(RECORD_COUNT)],
            id="composite key",
        ),
        pytest.param(
            ["LearnerId", "Step"],
            [
                {"LearnerId": i // 3, "Step": f"Step {i % 3}"}
                for i in range(RECORD_COUNT)
            ],
            id="composite string key",
        ),
    ],
)
def test_full_table_sync_gets_every_record(primary_keys, records):
    """Page on the primary keys, which Aptem serves fast at any depth."""
    tap = TapAptem(
        config={"tenant_name": "tenant", "api_token": "token"},
        setup_mapper=False,
    )
    stream = AptemODataStream(
        tap,
        name="Reviews",
        schema={"properties": {key: {} for key in primary_keys}},
        path="/Reviews",
    )
    stream.primary_keys = primary_keys
    stream.requests_session.mount("https://", ODataV1Adapter(records[::-1]))

    assert list(stream.get_records(context=None)) == records  # noqa: S101


def test_embedded_collection_sync_gets_every_record():
    """Page on the parent keys, past a page of parents with no embedded records."""
    parents = [
        {
            "Id": i,
            "UserTrackers_Trackers": [] if i in range(10_000, 20_000) else [{"Id": i}],
        }
        for i in range(20_010)
    ]

    tap = TapAptem(
        config={"tenant_name": "tenant", "api_token": "token"},
        setup_mapper=False,
    )
    stream = EmbeddedCollectionStream(
        tap=tap,
        name="UserTrackers_Trackers",
        schema={"properties": {"Id": {}, "UserId": {}}},
        path="/Users",
        parent_key_map={"Id": "UserId"},
    )
    stream.primary_keys = ["Id", "UserId"]
    stream.requests_session.mount("https://", ODataV1Adapter(parents[::-1]))

    assert list(stream.get_records(context=None)) == [  # noqa: S101
        {"UserId": parent["Id"]} | tracker
        for parent in parents
        for tracker in parent["UserTrackers_Trackers"]
    ]
