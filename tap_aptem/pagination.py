"""Pagination classes for tap-aptem."""

from __future__ import annotations

import typing as t

from singer_sdk.pagination import BaseAPIPaginator
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
