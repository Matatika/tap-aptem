"""REST client handling, including ODataStream base class."""

from __future__ import annotations

from datetime import datetime
from http import HTTPStatus
from typing import TYPE_CHECKING

from singer_sdk.authenticators import APIKeyAuthenticator
from singer_sdk.pagination import BaseOffsetPaginator
from singer_sdk.streams import RESTStream, Stream
from typing_extensions import override

from tap_aptem import hiddendict
from tap_aptem.pagination import CallbackPaginator

if TYPE_CHECKING:
    import requests


ENTITY_RECORD_LIMITS = {
    "LearningPlanEvidences": 5000,
    "ReviewResponses": 5000,
    "Users": 1000,
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

    entity_name: str

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
        if not self.replication_key:
            return BaseOffsetPaginator(start_value=0, page_size=self.page_size)

        def get_replication_key_value(response: requests.Response):  # noqa: ARG001
            state = self.get_context_state(self.context)

            if replication_key_value := state.get("replication_key_value"):
                return datetime.fromisoformat(replication_key_value)

            return None

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
        elif isinstance(next_page_token, datetime):
            params["$filter"] = (
                f"{self.replication_key} gt {next_page_token.isoformat()}"
            )

        if selected_child_streams := [
            cs.name for cs in self.child_streams if cs.selected
        ]:
            params["$expand"] = ",".join(selected_child_streams)

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
        if response.status_code == HTTPStatus.FORBIDDEN:
            msg = self.response_error_message(response)
            raise _ResumableAPIError(msg, response)

        if response.status_code == HTTPStatus.REQUEST_URI_TOO_LONG:
            self.logger.error(
                "Too many properties requested - reduce selection and try again"
            )

        super().validate_response(response)

    @override
    def get_child_context(self, record, context):
        if not self.child_streams:
            return super().get_child_context(record, context)

        return {
            **{self.entity_name + pk: record[pk] for pk in self.primary_keys},
            self.entity_name: hiddendict(record),
        }


class EmbeddedCollectionStream(Stream):
    """Embedded collection stream for inline related resources."""

    state_partitioning_keys = ()  # do not store any state bookmarks

    parent_entity_name: str
    collection_name: str

    @override
    def get_records(self, context):
        base_record = {**context}

        for record in base_record.pop(self.parent_entity_name)[self.collection_name]:
            yield base_record | record
