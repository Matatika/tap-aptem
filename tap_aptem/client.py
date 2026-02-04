"""REST client handling, including ODataStream base class."""

from __future__ import annotations

from http import HTTPStatus
from typing import TYPE_CHECKING

from singer_sdk.authenticators import APIKeyAuthenticator
from singer_sdk.pagination import BaseOffsetPaginator
from singer_sdk.streams import RESTStream
from typing_extensions import override

if TYPE_CHECKING:
    import requests


class _ResumableAPIError(Exception):
    def __init__(self, message: str, response: requests.Response) -> None:
        super().__init__(message)
        self.response = response


class AptemODataStream(RESTStream):
    """Aptem OData stream class."""

    page_size = 100_000
    records_jsonpath = "$.value[*]"

    # timestamps are sometimes returned with different ms grains causing the sorted
    # check (str > str) to fail, despite being ordered correctly
    #
    # >>> "2025-11-25T10:57:52.6880167Z" > "2025-11-25T10:57:52.68Z"
    # False
    check_sorted = False

    @override
    @property
    def is_sorted(self):
        return bool(self.replication_key)

    @override
    @property
    def url_base(self):
        tenant_name = self.config["tenant_name"]
        return f"https://{tenant_name}.aptem.co.uk/odata/1.0"

    @override
    @property
    def authenticator(self):
        return APIKeyAuthenticator(
            key="X-API-Token",
            value=self.config["api_token"],
        )

    @override
    def get_records(self, context):
        try:
            yield from super().get_records(context)
        except _ResumableAPIError as e:
            self.logger.warning(e)

    @override
    def get_new_paginator(self):
        return BaseOffsetPaginator(start_value=0, page_size=self.page_size)

    @override
    def get_url_params(self, context, next_page_token):
        params = super().get_url_params(context, next_page_token)
        params["$top"] = self.page_size
        if next_page_token is not None:
            params["$skip"] = next_page_token

        if self.replication_key:
            params["$orderby"] = self.replication_key

        if starting_timestamp := self.get_starting_timestamp(context):
            params["$filter"] = (
                f"{self.replication_key} ge {starting_timestamp.isoformat()}"
            )

        return params

    @override
    def validate_response(self, response):
        if response.status_code == HTTPStatus.FORBIDDEN or (
            response.status_code == HTTPStatus.BAD_REQUEST
            and "An error occured while executing requested action"
            in response.json()["error"]["message"]
        ):
            msg = self.response_error_message(response)
            raise _ResumableAPIError(msg, response)

        super().validate_response(response)
