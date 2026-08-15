"""Unit coverage for JetStream configuration reconciliation."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import Any

from nats.js import api as js_api
from nats.js import errors as js_errors

from loafer.adapters.queue import jetstream
from loafer.adapters.queue.jetstream import JetStreamTransport
from loafer.core.roles import stream_name, stream_subjects


class FakeJetStream:
    def __init__(self, config: js_api.StreamConfig) -> None:
        self._config = config
        self.updated: js_api.StreamConfig | None = None

    async def stream_info(self, name: str) -> Any:
        assert name == stream_name()
        return SimpleNamespace(config=self._config)

    async def update_stream(self, config: js_api.StreamConfig) -> None:
        self.updated = config


def test_stream_discard_policy_is_reconciled_to_discard_new() -> None:
    existing = js_api.StreamConfig(
        name=stream_name(),
        subjects=list(stream_subjects()),
        retention=js_api.RetentionPolicy.WORK_QUEUE,
        duplicate_window=jetstream._DEDUPE_WINDOW_SECONDS,
        max_age=jetstream._MAX_JOB_AGE_SECONDS,
        max_bytes=jetstream._MAX_STREAM_BYTES,
        max_msg_size=jetstream._MAX_JOB_BYTES,
        storage=js_api.StorageType.FILE,
        discard=js_api.DiscardPolicy.OLD,
    )
    fake = FakeJetStream(existing)
    transport = object.__new__(JetStreamTransport)
    transport._api = js_api
    transport._errors = js_errors
    transport._js = fake

    asyncio.run(transport._ensure_stream())

    assert fake.updated is not None
    assert fake.updated.discard is js_api.DiscardPolicy.NEW
