"""REST client handling, including ODataStream base class."""

from __future__ import annotations

from http import HTTPStatus
from typing import TYPE_CHECKING

from singer_sdk.authenticators import APIKeyAuthenticator
from singer_sdk.exceptions import RetriableAPIError
from singer_sdk.pagination import BaseOffsetPaginator
from singer_sdk.streams import RESTStream
from typing_extensions import override

from tap_aptem.pagination import CallbackPaginator

if TYPE_CHECKING:
    import requests


ENTITY_RECORD_LIMITS = {
    "LearningPlanEvidences": 5000,
    "ReviewResponses": 5000,
}


class _ResumableAPIError(Exception):
    def __init__(self, message: str, response: requests.Response) -> None:
        super().__init__(message)
        self.response = response


class AptemODataStream(RESTStream):
    """Aptem OData stream class."""

    records_jsonpath = "$.value[*]"

    # timestamps are sometimes returned with different ms grains causing the sorted
    # check (str > str) to fail, despite being ordered correctly
    #
    # >>> "2025-11-25T10:57:52.6880167Z" > "2025-11-25T10:57:52.68Z"
    # False
    check_sorted = False

    @property
    def page_size(self):
        """Number of entity records to request at a time."""
        return ENTITY_RECORD_LIMITS.get(self.name, 100_000)

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
        if self.replication_method == "FULL_TABLE" or not self.replication_key:
            return BaseOffsetPaginator(start_value=0, page_size=self.page_size)

        def get_replication_key_value(response: requests.Response):  # noqa: ARG001
            state = self.get_context_state(self.context)
            return state.get("replication_key_value")

        return CallbackPaginator(get_replication_key_value)

    @override
    def get_url_params(self, context, next_page_token):
        params = super().get_url_params(context, next_page_token)
        params["$top"] = self.page_size

        if self.replication_key:
            params["$orderby"] = self.replication_key

        if starting_timestamp := self.get_starting_timestamp(context):
            params["$filter"] = (
                f"{self.replication_key} ge {starting_timestamp.isoformat()}"
            )

        if isinstance(next_page_token, int):
            params["$skip"] = next_page_token
        elif isinstance(next_page_token, str):
            params["$filter"] = f"{self.replication_key} gt {next_page_token}"

        selected_columns = [
            column_name
            for column_name in self.schema["properties"]
            if self.mask[("properties", column_name)]
        ]

        if selected_columns:
            params["$select"] = ",".join(selected_columns)

        return params

    @override
    def validate_response(self, response):
        if (
            response.status_code == HTTPStatus.BAD_REQUEST
            and "Try again" in response.json()["error"]["message"]
        ):
            msg = self.response_error_message(response)
            raise RetriableAPIError(msg, response)

        if response.status_code == HTTPStatus.FORBIDDEN:
            msg = self.response_error_message(response)
            raise _ResumableAPIError(msg, response)

        if response.status_code == HTTPStatus.REQUEST_URI_TOO_LONG:
            self.logger.error(
                "Too many properties requested - reduce selection and try again"
            )

        super().validate_response(response)

    @override
    def _increment_stream_state(self, latest_record, *, context=None):
        # avoid "New replication value is null" log pollution when attempting to
        # increment state for records missing a replication value for streams with a
        # defined replication key
        if self.replication_key and not latest_record.get(self.replication_key):
            if not hasattr(self, "_has_null_replication_values"):
                self.logger.warning("Stream has null replication values")

                # doesn't actually matter what this value is; we just need to set the
                # attribute
                self._has_null_replication_values = True

            return None

        return super()._increment_stream_state(latest_record, context=context)


class EmbeddedCollectionStream(AptemODataStream):
    """Embedded collection stream for inline related resources."""

    @override
    def __init__(self, /, parent_entity_name: str, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.parent_entity_name = parent_entity_name

    @override
    def get_url_params(self, context, next_page_token):
        params = super().get_url_params(context, next_page_token)

        # select only the parent entity primary keys
        params["$select"] = ",".join(
            pk.removeprefix(self.parent_entity_name)
            for pk in self.primary_keys
            if pk.startswith(self.parent_entity_name)
        )

        # expand the embedded collection
        params["$expand"] = self.name

        return params

    @override
    def parse_response(self, response):
        for record in super().parse_response(response):
            for collection_properties in record.pop(self.name):
                # prefix parent properties with the parent entity name
                parent_properties = {
                    self.parent_entity_name + k: v for k, v in record.items()
                }

                yield parent_properties | collection_properties
