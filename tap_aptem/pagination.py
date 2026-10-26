# Copyright (c) 2026 Meltano.

"""Pagination classes for tap-aptem."""

from __future__ import annotations

import typing as t

from singer_sdk.pagination import BaseAPIPaginator, BaseHATEOASPaginator
from typing_extensions import override

if t.TYPE_CHECKING:
    import requests


class CallbackPaginator(BaseAPIPaginator):
    """Paginator class that invokes a callback function to get the next value."""

    @override
    def __init__(self, get_next_value: t.Callable[[requests.Response], t.Any]) -> None:
        super().__init__(None)
        self.get_next_value = get_next_value

    @override
    def get_next(self, response):
        return self.get_next_value(response)


class ODataNextLinkPaginator(BaseHATEOASPaginator):
    """Paginator class that follows the OData `@odata.nextLink` of each page."""

    @override
    def get_next_url(self, response):
        return response.json().get("@odata.nextLink")


class LastRecordPaginator(BaseAPIPaginator):
    """Paginator class that returns the last record of each OData page."""

    def __init__(self) -> None:
        """Create a paginator that starts with no record."""
        super().__init__(None)

    @override
    def continue_if_empty(self, response):
        # a page of parent records can expand to no embedded records
        return bool(response.json()["value"])

    @override
    def get_next(self, response):
        return response.json()["value"][-1]
