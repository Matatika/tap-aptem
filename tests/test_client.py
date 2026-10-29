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

from tap_aptem import client
from tap_aptem.client import AptemODataStream, AptemODataV2Stream
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


def _holds(review, field, operator, value):
    if value == "null":
        return review[field] is None

    if review[field] is None:
        return False

    return review[field] > value if operator == "gt" else review[field] >= value


def _matches(review, condition):
    return any(
        all(
            _holds(review, field, operator, value)
            for field, operator, value in (
                term.split(" ") for term in group.split(" and ")
            )
        )
        for group in condition.strip("()").split(" or ")
    )


class ReviewsAdapter(BaseAdapter):
    """Serve reviews with the OData filter, order, and page of each request."""

    def __init__(self, reviews) -> None:
        """Serve these reviews."""
        super().__init__()
        self.reviews = reviews

    def send(self, request, **kwargs):  # noqa: ARG002, D102
        query = {k: v[0] for k, v in parse_qs(urlparse(request.url).query).items()}
        skip = int(query.get("$skip", 0))

        reviews = sorted(
            (
                review
                for review in self.reviews
                if "$filter" not in query or _matches(review, query["$filter"])
            ),
            key=lambda review: tuple(
                review[key] or "" for key in query["$orderby"].split(",")
            ),
        )

        body = {"value": reviews[skip : skip + int(query["$top"])]}

        response = requests.Response()
        response.status_code = 200
        response.raw = io.BytesIO(json.dumps(body).encode())
        response.request = request
        return response

    def close(self):  # noqa: D102
        pass


def _review(review_id, created_day, updated_day=None):
    return {
        "Id": review_id,
        "CreatedDate": f"2026-01-{created_day:02}T00:00:00+00:00",
        "UpdatedDate": updated_day and f"2026-01-{updated_day:02}T00:00:00+00:00",
    }


def _sync(tap, capsys):
    tap.sync_all()

    messages = [json.loads(line) for line in capsys.readouterr().out.splitlines()]

    return [message["record"] for message in messages if message["type"] == "RECORD"]


def test_reviews_sync_incrementally(monkeypatch, capsys):
    """Sync a review on its `UpdatedDate`, or its `CreatedDate` where that is null."""
    monkeypatch.setitem(client.V1_ENTITY_RECORD_LIMITS, "Reviews", 2)

    reviews = [
        _review(1, created_day=1, updated_day=4),
        _review(2, created_day=2),
        _review(3, created_day=3, updated_day=5),
        _review(4, created_day=4),
        _review(5, created_day=5),
    ]

    def discover_streams(tap):
        datetime_type = {"type": ["string", "null"], "format": "date-time"}
        stream = AptemODataStream(
            tap,
            name="Reviews",
            schema={
                "properties": {
                    "Id": {"type": "integer"},
                    "CreatedDate": datetime_type,
                    "UpdatedDate": datetime_type,
                },
            },
            path="/Reviews",
        )
        stream.primary_keys = ("Id",)
        stream.replication_key = "UpdatedDate"
        stream.requests_session.mount("https://", ReviewsAdapter(reviews))
        return [stream]

    monkeypatch.setattr(TapAptem, "discover_streams", discover_streams)
    tap = TapAptem(config={"tenant_name": "tenant", "api_token": "token"})

    records = _sync(tap, capsys)

    assert [record["Id"] for record in records] == [1, 2, 3, 4, 5]  # noqa: S101

    reviews[1] = _review(2, created_day=2, updated_day=6)
    reviews.append(_review(6, created_day=7))
    reviews.append(_review(7, created_day=7, updated_day=1))

    records = _sync(tap, capsys)

    assert [record["Id"] for record in records] == [2, 3, 5, 6]  # noqa: S101

    records = _sync(tap, capsys)

    assert [record["Id"] for record in records] == [2, 6]  # noqa: S101


def test_catalog_replication_key_syncs_alone(monkeypatch, capsys):
    """Sync on a catalog replication key for a stream with no configured key."""
    records = [{"Id": i, "Date": f"2026-01-{i:02}T00:00:00+00:00"} for i in range(1, 6)]

    def discover_streams(tap):
        stream = AptemODataStream(
            tap,
            name="HoursRecords",
            schema={
                "properties": {
                    "Id": {"type": "integer"},
                    "Date": {"type": ["string", "null"], "format": "date-time"},
                },
            },
            path="/HoursRecords",
        )
        stream.requests_session.mount("https://", ReviewsAdapter(records))
        return [stream]

    monkeypatch.setattr(TapAptem, "discover_streams", discover_streams)
    config = {"tenant_name": "tenant", "api_token": "token"}

    catalog = TapAptem(config=config).catalog_dict
    catalog["streams"][0] |= {
        "replication_key": "Date",
        "replication_method": "INCREMENTAL",
    }

    tap = TapAptem(
        config=config,
        catalog=catalog,
        state={
            "bookmarks": {
                "HoursRecords": {
                    "replication_key": "Date",
                    "replication_key_value": "2026-01-03T00:00:00+00:00",
                },
            },
        },
    )

    records = _sync(tap, capsys)

    assert [record["Id"] for record in records] == [3, 4, 5]  # noqa: S101


def _evaluate(expression, record):
    """Evaluate an OData `$filter` of comparisons, `and`, `or` and parentheses."""
    tokens = expression.replace("(", " ( ").replace(")", " ) ").split()

    def parse_or(i):
        result, i = parse_and(i)
        while i < len(tokens) and tokens[i] == "or":
            right, i = parse_and(i + 1)
            result = result or right
        return result, i

    def parse_and(i):
        result, i = parse_term(i)
        while i < len(tokens) and tokens[i] == "and":
            right, i = parse_term(i + 1)
            result = result and right
        return result, i

    def parse_term(i):
        if tokens[i] == "(":
            result, i = parse_or(i + 1)
            return result, i + 1

        field, operator, literal = tokens[i : i + 3]
        value = record[field]
        other = None if literal == "null" else type(value or "")(literal)

        if operator in ("eq", "ne"):
            return (value == other) == (operator == "eq"), i + 3

        if value is None or other is None:
            return False, i + 3

        compare = {"gt": value > other, "ge": value >= other, "le": value <= other}
        return compare[operator], i + 3

    return parse_or(0)[0]


class FilteringAdapter(BaseAdapter):
    """Serve records with the OData filter, order, and page of each request."""

    def __init__(self, records) -> None:
        """Serve these records."""
        super().__init__()
        self.records = records

    def send(self, request, **kwargs):  # noqa: ARG002, D102
        query = {k: v[0] for k, v in parse_qs(urlparse(request.url).query).items()}
        skip = int(query.get("$skip", 0))

        records = sorted(
            (
                record
                for record in self.records
                if "$filter" not in query or _evaluate(query["$filter"], record)
            ),
            key=lambda record: tuple(
                (record[key] is not None, record[key] or "")
                for key in query["$orderby"].split(",")
            ),
        )

        body = {"value": records[skip : skip + int(query["$top"])]}

        response = requests.Response()
        response.status_code = 200
        response.raw = io.BytesIO(json.dumps(body).encode())
        response.request = request
        return response

    def close(self):  # noqa: D102
        pass


def test_sync_gets_every_record_sharing_a_replication_key_value(monkeypatch, capsys):
    """Get every record that shares a replication key value across a page end."""
    monkeypatch.setitem(client.V1_ENTITY_RECORD_LIMITS, "Tasks", 3)

    def task(task_id, updated_date):
        return {"Id": task_id, "UpdatedDate": updated_date}

    tasks = [
        task(1, None),
        task(2, None),
        task(3, None),
        task(4, None),
        task(5, "2026-01-01T00:00:00+00:00"),
        # a page ends partway through these records, which share a value
        *(task(task_id, "2026-01-02T00:00:00+00:00") for task_id in range(6, 13)),
        task(13, "2026-01-03T00:00:00+00:00"),
    ]

    def discover_streams(tap):
        stream = AptemODataStream(
            tap,
            name="Tasks",
            schema={
                "properties": {
                    "Id": {"type": "integer"},
                    "UpdatedDate": {"type": ["string", "null"], "format": "date-time"},
                },
            },
            path="/Tasks",
        )
        stream.primary_keys = ("Id",)
        stream.replication_key = "UpdatedDate"
        stream.requests_session.mount("https://", FilteringAdapter(tasks))
        return [stream]

    monkeypatch.setattr(TapAptem, "discover_streams", discover_streams)
    tap = TapAptem(config={"tenant_name": "tenant", "api_token": "token"})

    records = _sync(tap, capsys)

    assert [record["Id"] for record in records] == list(range(1, 14))  # noqa: S101

    # the next sync starts from the bookmark, inclusive
    tasks.extend(task(task_id, "2026-01-03T00:00:00+00:00") for task_id in (14, 15))

    records = _sync(tap, capsys)

    assert [record["Id"] for record in records] == [13, 14, 15]  # noqa: S101
