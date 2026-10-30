# Copyright (c) 2026 Meltano.

"""Pagination classes for tap-aptem."""

from __future__ import annotations

from typing import NamedTuple

from singer_sdk.pagination import BaseAPIPaginator, BaseHATEOASPaginator
from typing_extensions import override


class ReplicationKeyToken(NamedTuple):
    """The last replication key value and the records with it already received."""

    value: str | None
    received: int


class ReplicationKeyPaginator(BaseAPIPaginator[ReplicationKeyToken]):
    """Paginator class that pages from the last replication key value of each page."""

    def __init__(self, replication_key: str) -> None:
        """Page on this replication key."""
        super().__init__(ReplicationKeyToken(value=None, received=0))
        self.replication_key = replication_key

    @override
    def get_next(self, response):
        values = [record[self.replication_key] for record in response.json()["value"]]
        value = values[-1]
        received = values.count(value)

        # a page of only the previous value follows the records already received
        if value == self.current_value.value:
            received += self.current_value.received

        return ReplicationKeyToken(value, received)


class ODataNextLinkPaginator(BaseHATEOASPaginator):
    """Paginator class that follows the OData `@odata.nextLink` of each page."""

    @override
    def get_next_url(self, response):
        return response.json().get("@odata.nextLink")
