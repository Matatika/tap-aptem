# Copyright (c) 2026 Meltano.

"""Pagination classes for tap-aptem."""

from __future__ import annotations

from typing import NamedTuple

from singer_sdk.pagination import BaseAPIPaginator, BaseHATEOASPaginator
from typing_extensions import override


class ReplicationKeyToken(NamedTuple):
    """The last replication key value and the records with it to skip."""

    value: str | None
    skip: int


class ReplicationKeyPaginator(BaseAPIPaginator[ReplicationKeyToken]):
    """Paginator class that pages from the last replication key value of each page."""

    def __init__(self, replication_key: str) -> None:
        """Page on this replication key."""
        super().__init__(ReplicationKeyToken(value=None, skip=0))
        self.replication_key = replication_key

    @override
    def get_next(self, response):
        records = response.json()["value"]
        first_value = records[0][self.replication_key]
        value = records[-1][self.replication_key]

        # a record that changes before the next page shifts a skip, so skip only past
        # a page of one value, which `ge` alone would request again
        if first_value != value:
            return ReplicationKeyToken(value, skip=0)

        skip = len(records)

        # a page of only the previous value follows the records already skipped
        if value == self.current_value.value:
            skip += self.current_value.skip

        return ReplicationKeyToken(value, skip)


class ODataNextLinkPaginator(BaseHATEOASPaginator):
    """Paginator class that follows the OData `@odata.nextLink` of each page."""

    @override
    def get_next_url(self, response):
        return response.json().get("@odata.nextLink")
