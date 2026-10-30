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


def _task(task_id, updated_day=None):
    return {
        "Id": task_id,
        "UpdatedDate": updated_day and f"2026-01-{updated_day:02}T00:00:00+00:00",
    }


def _tasks_tap(monkeypatch, adapter):
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
        stream.requests_session.mount("https://", adapter)
        return [stream]

    monkeypatch.setattr(TapAptem, "discover_streams", discover_streams)
    return TapAptem(config={"tenant_name": "tenant", "api_token": "token"})


def test_sync_gets_every_record_sharing_a_replication_key_value(monkeypatch, capsys):
    """Get every record that shares a replication key value across page ends."""
    monkeypatch.setitem(client.V1_ENTITY_RECORD_LIMITS, "Tasks", 3)

    tasks = [
        *(_task(task_id) for task_id in range(1, 5)),
        _task(5, updated_day=1),
        # more records share this value than a page holds
        *(_task(task_id, updated_day=2) for task_id in range(6, 13)),
        _task(13, updated_day=3),
    ]
    tap = _tasks_tap(monkeypatch, ReviewsAdapter(tasks))

    records = _sync(tap, capsys)

    # `ge` sends the last record of a page that ends partway through its values again
    assert [record["Id"] for record in records] == [  # noqa: S101
        *range(1, 7),
        *range(6, 14),
    ]

    tasks.extend(_task(task_id, updated_day=3) for task_id in (14, 15))

    records = _sync(tap, capsys)

    assert [record["Id"] for record in records] == [13, 14, 15]  # noqa: S101


class ChangingTasksAdapter(ReviewsAdapter):
    """Serve tasks, and update the second task after each request."""

    def send(self, request, **kwargs):  # noqa: D102
        response = super().send(request, **kwargs)
        self.reviews[1] = _task(2, updated_day=5)
        return response


def test_sync_gets_every_record_when_a_page_end_changes(monkeypatch, capsys):
    """Get every record when the last record of a page changes before the next page."""
    monkeypatch.setitem(client.V1_ENTITY_RECORD_LIMITS, "Tasks", 2)

    tasks = [_task(task_id, updated_day=task_id) for task_id in range(1, 5)]
    tap = _tasks_tap(monkeypatch, ChangingTasksAdapter(tasks))

    records = _sync(tap, capsys)

    assert {record["Id"] for record in records} == {1, 2, 3, 4}  # noqa: S101
