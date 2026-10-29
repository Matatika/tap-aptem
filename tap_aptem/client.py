# Copyright (c) 2026 Meltano.

"""REST client handling, including ODataStream base class."""

from __future__ import annotations

from http import HTTPStatus
from typing import TYPE_CHECKING

import backoff
from singer_sdk.authenticators import APIKeyAuthenticator
from singer_sdk.exceptions import RetriableAPIError
from singer_sdk.pagination import BaseOffsetPaginator
from singer_sdk.streams import RESTStream
from typing_extensions import override

from tap_aptem.pagination import CallbackPaginator, ODataNextLinkPaginator

if TYPE_CHECKING:
    import requests


V1_ENTITY_RECORD_LIMITS = {
    "ActivityLog": 5000,
    "LearnerEmployment": 5000,
    "LearningPlanEvidences": 5000,
    "LearningPlanEvidenceWithActivityData": 5000,
    "ReviewResponses": 5000,
}

STREAM_REPLICATION_KEYS = {
    "AimWorkPlacements": ("WorkPlaceStartDate",),
    "ApprenticeshipFinancialRecords": ("Date",),
    "AptemCognitiveAssessments": ("DateStart",),
    "Actions": ("UpdatedDate",),
    "AwardingBodyQualifications": ("UpdatedDate",),
    "AwardingBodyQualificationsAssessmentHistory": ("Date",),
    "CheckpointAssessments": ("LastModifiedDate",),
    "ComplianceDocuments": ("Date",),
    "ComponentDueDateChanges": ("ChangeDate",),
    "CrmActivities": ("UpdatedDate",),
    "DestinationProgressions": ("StartDate",),
    "EmployerGroups": ("UpdatedDate",),
    "EPAErrors": ("Date",),
    "Episodes": ("UpdatedDate",),
    "Groups": ("UpdatedDate",),
    "HoursRecords": (),
    "IlrAims": ("UpdatedDate",),
    "IlrLearner": ("UpdatedDate",),
    "Jobs": ("UpdatedDate",),
    "LearnerEmployment": ("EmploymentStartDate",),
    "LearningPlanComponents": ("UpdatedDate",),
    "LearningPlanEvidences": ("UpdatedDate",),
    "LearningPlanEvidenceWithActivityData": ("UpdatedDate",),
    "MaximumProgrammeDataFields": (),
    "Messages": ("CreatedDate",),
    "Milestones": (),
    "Notes": ("Date",),
    "OnboardingResponses": ("Date",),
    "OrganizationContacts": (),
    "Organizations": ("UpdatedDate",),
    "ReviewResponses": ("UpdatedDate",),
    # Aptem leaves `UpdatedDate` null until a review is first updated
    "Reviews": ("UpdatedDate", "CreatedDate"),
    "Tasks": ("UpdatedDate",),
    "Trackers": ("UpdatedDate",),
    "Users": ("UpdatedDate",),
    "UserGroups": (),
    "VirtualAssistantExchanges": ("Date",),
    "WithdrawalReasons": ("DateAdded",),
}

# streams updated so frequently during working hours that records are changed faster
# than pages can be requested, so pagination never reaches an empty page - bound the
# sync to records updated before the sync started (picked up by the next sync instead)
BOUNDED_SYNC_STREAMS = {
    "LearningPlanComponents",
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
    @override
    @property
    def check_sorted(self):
        # an unsorted stream keeps the highest replication key value only if this is set
        return not self.is_sorted

    @property
    def page_size(self):
        """Number of entity records to request at a time."""
        return V1_ENTITY_RECORD_LIMITS.get(self.name, 10000)

    @property
    def replication_keys(self):
        """Replication key, then its fallbacks where the earlier keys are null.

        A record syncs if its first key with a value is on or after the bookmark. A key
        that a catalog sets in place of the configured key has no fallbacks.
        """
        keys = STREAM_REPLICATION_KEYS.get(self.name, ())

        if self.replication_key in keys[:1]:
            return keys

        return (self.replication_key,) if self.replication_key else ()

    @property
    def tiebreak_key(self):
        """Primary key that orders records sharing a replication key value.

        Only a single integer key, which an OData filter compares without quotes.
        """
        if len(self.primary_keys or ()) != 1:
            return None

        key = self.primary_keys[0]
        key_type = self.schema["properties"].get(key, {}).get("type")
        key_types = key_type if isinstance(key_type, list) else [key_type]

        return key if "integer" in key_types else None

    @override
    @property
    def is_sorted(self):
        # records are in replication key order only for a single key - with fallbacks,
        # the order puts the last fallback first, and a record with a null replication
        # key has no bookmark value
        return len(self.replication_keys) == 1

    @override
    @property
    def url_base(self):
        tenant_name = self.config["tenant_name"]
        odata_version = self.config["odata_version"]
        return f"https://{tenant_name}.aptem.co.uk/odata/{odata_version}"

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
        if self.replication_method == "FULL_TABLE" or not self.is_sorted:
            return BaseOffsetPaginator(start_value=0, page_size=self.page_size)

        if self.tiebreak_key:
            # page after the last record's replication key value *and* key: paging on
            # `gt` the replication key value alone skips the rest of the records that
            # share it when a page ends partway through them
            def get_last_record_position(response: requests.Response):
                # a page is only advanced past once it has records
                last_record = response.json()["value"][-1]
                return (
                    last_record.get(self.replication_key),
                    last_record[self.tiebreak_key],
                )

            return CallbackPaginator(get_last_record_position)

        def get_replication_key_value(response: requests.Response):  # noqa: ARG001
            state = self.get_context_state(self.context)
            return state.get("replication_key_value")

        return CallbackPaginator(get_replication_key_value)

    @override
    def get_url_params(self, context, next_page_token):
        params = super().get_url_params(context, next_page_token)
        params["$top"] = self.page_size

        if self.replication_keys:
            # records can page by offset, so order them first by the last fallback key,
            # which a change during the sync is least likely to alter
            order = [*reversed(self.replication_keys)]

            if self.is_sorted and self.tiebreak_key:
                order.append(self.tiebreak_key)

            params["$orderby"] = ",".join(order)

        if isinstance(next_page_token, int):
            params["$skip"] = next_page_token

        filters = []

        if isinstance(next_page_token, tuple):
            filters.append(self._after_position_filter(*next_page_token))

        if isinstance(next_page_token, str):
            filters.append(f"{self.replication_key} gt {next_page_token}")
        elif starting_timestamp := self.get_starting_timestamp(context):
            keys = self.replication_keys
            key_filters = (
                " and ".join(
                    [
                        *(f"{null_key} eq null" for null_key in keys[:i]),
                        f"{key} ge {starting_timestamp.isoformat()}",
                    ]
                )
                for i, key in enumerate(keys)
            )
            filters.append(f"({' or '.join(key_filters)})")

        if self.name in BOUNDED_SYNC_STREAMS and (
            signpost := self.get_replication_key_signpost(context)
        ):
            filters.append(f"{self.replication_key} le {signpost.isoformat()}")

        if filters:
            params["$filter"] = " and ".join(filters)

        selected_columns = [
            column_name
            for column_name in self.schema["properties"]
            if self.mask[("properties", column_name)]
        ]

        if selected_columns:
            params["$select"] = ",".join(selected_columns)

        return params

    def _after_position_filter(self, value, key_value):
        """Filter records ordered after a replication key value and key."""
        key = self.replication_key
        after_key = f"{self.tiebreak_key} gt {key_value}"

        if value is None:
            # null replication key values are ordered first
            return f"({key} ne null or ({key} eq null and {after_key}))"

        return f"({key} gt {value} or ({key} eq {value} and {after_key}))"

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
    def backoff_wait_generator(self):
        default_wait_gen = backoff.expo(factor=10)

        # backoff primes only the generator that it calls, so prime the default here
        next(default_wait_gen)

        exception = yield

        while True:
            if (
                isinstance(exception, RetriableAPIError)
                and exception.response.status_code == HTTPStatus.TOO_MANY_REQUESTS
                and (retry_after := exception.response.headers.get("Retry-After"))
            ):
                exception = yield float(retry_after)
            else:
                exception = yield next(default_wait_gen)

    @override
    def backoff_max_tries(self):
        return 4

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


class AptemODataV2Stream(AptemODataStream):
    """Aptem OData v2 stream class."""

    @override
    def get_new_paginator(self):
        return ODataNextLinkPaginator()

    @override
    def get_url_params(self, context, next_page_token):
        # a next link already carries the query of the first request
        if next_page_token:
            return {}

        params = super().get_url_params(context, next_page_token)

        # OData v2 applies `$top` to the whole result set, not to one page
        del params["$top"]

        return params

    @override
    def get_http_request(self, *, page):
        request = super().get_http_request(page=page)

        if page.next_page_token:
            request.url = page.next_page_token.geturl()

        return request


class EmbeddedCollectionStream(AptemODataStream):
    """Embedded collection stream for inline related resources."""

    @override
    def __init__(self, /, parent_key_map: dict[str, str], *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.parent_key_map = parent_key_map

    @property
    def tiebreak_key(self):
        """No key: a response holds parent entities, selected down to their keys."""
        return None

    @override
    def get_url_params(self, context, next_page_token):
        params = super().get_url_params(context, next_page_token)

        # select only the parent entity keys
        params["$select"] = ",".join(self.parent_key_map)

        # expand the embedded collection
        params["$expand"] = self.name

        return params

    @override
    def parse_response(self, response):
        for record in super().parse_response(response):
            for collection_properties in record.pop(self.name):
                # namespace parent properties to match the discovered schema
                parent_properties = {
                    self.parent_key_map[k]: v for k, v in record.items()
                }

                yield parent_properties | collection_properties
