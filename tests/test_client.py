# Copyright (c) 2026 Meltano.

"""Tests for the Aptem OData stream class."""

from http import HTTPStatus

import pytest
import requests
from singer_sdk.exceptions import RetriableAPIError

from tap_aptem.client import AptemODataStream
from tap_aptem.tap import TapAptem


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
            [2, 4],
            id="rate limited without Retry-After",
        ),
        pytest.param(
            HTTPStatus.SERVICE_UNAVAILABLE,
            {"Retry-After": "30"},
            [2, 4],
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
