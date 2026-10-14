"""Shared pytest fixtures."""

from __future__ import annotations

import json

import pytest

from apexbet.common.storage import LocalObjectStore
from apexbet.generator import AnomalyInjection, GeneratorConfig, SyntheticEventGenerator
from apexbet.transports import build_transport


@pytest.fixture()
def tmp_store(tmp_path) -> LocalObjectStore:
    return LocalObjectStore(str(tmp_path / "lake"))


@pytest.fixture()
def memory_transport():
    return build_transport("memory")


@pytest.fixture()
def clean_events():
    gen = SyntheticEventGenerator(
        GeneratorConfig(
            seed=42, num_accounts=200, num_matches=20, anomalies=AnomalyInjection.clean()
        )
    )
    return list(gen.generate(2000))


@pytest.fixture()
def dirty_events():
    gen = SyntheticEventGenerator(
        GeneratorConfig(
            seed=42, num_accounts=200, num_matches=20, anomalies=AnomalyInjection.demo()
        )
    )
    return list(gen.generate(2000))


@pytest.fixture()
def published_pipeline(memory_transport, tmp_store, clean_events):
    """A pipeline with a batch of clean events already published."""
    from apexbet.consumers import Pipeline

    memory_transport.publish_many(
        "test.topic",
        [(e["partition_key"], json.dumps(e).encode()) for e in clean_events],
    )
    return (
        Pipeline(
            memory_transport,
            tmp_store,
            source_topic="test.topic",
            dlq_topic="test.dlq",
            group_id="test-group",
            dedup_window_seconds=3600,
        ),
        memory_transport,
        tmp_store,
        clean_events,
    )
