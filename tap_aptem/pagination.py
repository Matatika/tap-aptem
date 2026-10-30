# Copyright (c) 2026 Meltano.

"""Pagination classes for tap-aptem."""

from __future__ import annotations

from singer_sdk.pagination import BaseAPIPaginator, BaseHATEOASPaginator
from typing_extensions import override


class ReplicationKeyPaginator(BaseAPIPaginator):
    """Paginator class that pages from the last replication key value of each page.

    Each value is the last replication key value and the number of records with it
    already received, which the next page skips.
    """

    def __init__(self, replication_key: str) -> None:
        """Page on this replication key."""
        super().__init__(None)
        self.replication_key = replication_key

    @override
    def get_next(self, response):
        values = [record[self.replication_key] for record in response.json()["value"]]
        value = values[-1]
        received = values.count(value)

        # a page of only the previous value follows the records that it skipped
        if self.current_value and self.current_value[0] == value:
            received += self.current_value[1]

        return value, received


class ODataNextLinkPaginator(BaseHATEOASPaginator):
    """Paginator class that follows the OData `@odata.nextLink` of each page."""

    @override
    def get_next_url(self, response):
        return response.json().get("@odata.nextLink")
