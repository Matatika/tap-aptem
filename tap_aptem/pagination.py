"""Pagination classes for tap-aptem."""

from __future__ import annotations

import datetime
import typing as t

from singer_sdk.helpers._compat import datetime_fromisoformat
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


class DateChunkPaginator(BaseAPIPaginator):
    """Rolling date-range cursor paginator.

    Every request is built from the current cursor as `ge cursor and lt cursor +
    chunk_days`. After a response with records, the cursor re-anchors to the
    highest replication-key value seen. After an empty response, the cursor jumps
    forward by `chunk_days`, or pagination stops if that jump would reach `now`.
    """

    @override
    def __init__(
        self,
        cursor: str | None,
        chunk_days: int,
        get_replication_key_value: t.Callable[[requests.Response], t.Any],
        now: datetime.datetime,
    ) -> None:
        super().__init__(cursor)
        self.chunk_days = chunk_days
        self.get_replication_key_value = get_replication_key_value
        self.now = now

    def _next_chunk_start(self) -> datetime.datetime:
        cursor = datetime_fromisoformat(self.current_value)
        return cursor + datetime.timedelta(days=self.chunk_days)

    @override
    def continue_if_empty(self, response):
        if self.current_value is None:
            return False

        return self._next_chunk_start() < self.now

    @override
    def get_next(self, response):
        if last_seen_value := self.get_replication_key_value(response):
            return last_seen_value

        if self.current_value is None:
            return None

        next_chunk_start = self._next_chunk_start()

        if next_chunk_start >= self.now:
            return None

        return next_chunk_start.isoformat()
