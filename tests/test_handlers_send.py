from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from console.converters.state_converter import load_default_state
from console.handlers import (
    on_clickhouse_send,
    on_kafka_send,
    purge_removed_logs,
    send_clickhouse_use_case,
    send_kafka_use_case,
    store_logs,
)
from console.persistence import SaveMeta


CLEAN_META = SaveMeta(dirty=False, last_saved_at=None, last_message="loaded")


@pytest.fixture
def isolated_cwd(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.chdir(tmp_path)
    return tmp_path


def test_purge_removed_logs_drops_missing_ids() -> None:
    logs = {"a": "A", "b": "B", "c": "C"}
    result = purge_removed_logs(logs, {"a", "b", "c"}, {"b"})
    assert result == {"b": "B"}


def test_purge_removed_logs_keeps_unchanged_when_no_removals() -> None:
    logs = {"a": "A"}
    result = purge_removed_logs(logs, {"a"}, {"a"})
    assert result == {"a": "A"}


def test_store_logs_sets_text() -> None:
    result = store_logs({}, "id-1", "line1\nline2")
    assert result == {"id-1": "line1\nline2"}


@pytest.mark.asyncio
async def test_send_kafka_use_case_success() -> None:
    state = load_default_state()
    uc = state.kafka_use_cases[0]
    producer = AsyncMock()
    producer.start = AsyncMock()
    producer.stop = AsyncMock()

    with patch("aiokafka.AIOKafkaProducer", return_value=producer):
        new_state, logs, status, text = await send_kafka_use_case(
            state,
            {},
            uc.id,
            uc.bootstrap_url,
            uc.topic,
            uc.hooks,
            uc.messages,
            float(uc.global_timeout_ms),
        )

    assert status.startswith("ok: sent")
    assert uc.id in logs
    assert "kafka producer start" in text
    producer.start.assert_awaited_once()
    producer.stop.assert_awaited_once()


@pytest.mark.asyncio
async def test_send_kafka_use_case_parse_error() -> None:
    state = load_default_state()
    uc = state.kafka_use_cases[0]
    _, logs, status, text = await send_kafka_use_case(
        state,
        {},
        uc.id,
        uc.bootstrap_url,
        uc.topic,
        uc.hooks,
        "not-json",
        float(uc.global_timeout_ms),
    )
    assert status.startswith("error:")
    assert "error:" in text
    assert uc.id in logs


@pytest.mark.asyncio
async def test_send_kafka_use_case_missing_aiokafka() -> None:
    state = load_default_state()
    uc = state.kafka_use_cases[0]
    with patch("builtins.__import__", side_effect=ImportError("no aiokafka")):
        _, logs, status, _ = await send_kafka_use_case(
            state,
            {},
            uc.id,
            uc.bootstrap_url,
            uc.topic,
            uc.hooks,
            uc.messages,
            float(uc.global_timeout_ms),
        )
    assert status.startswith("error:")
    assert uc.id in logs


def test_send_clickhouse_use_case_success() -> None:
    state = load_default_state()
    uc = state.clickhouse_use_cases[0]
    client = MagicMock()
    with patch("console.handlers.clickhouse_connect.get_client", return_value=client):
        _, logs, status, text = send_clickhouse_use_case(
            state,
            {},
            uc.id,
            uc.host,
            float(uc.port),
            uc.user,
            uc.password,
            uc.database,
            uc.hooks,
            uc.messages,
            float(uc.global_timeout_ms),
        )
    assert status.startswith("ok: sent")
    assert "clickhouse connect" in text
    client.close.assert_called_once()
    assert uc.id in logs


def test_send_clickhouse_use_case_sql_error() -> None:
    state = load_default_state()
    uc = state.clickhouse_use_cases[0]
    _, logs, status, _ = send_clickhouse_use_case(
        state,
        {},
        uc.id,
        uc.host,
        float(uc.port),
        uc.user,
        uc.password,
        uc.database,
        uc.hooks,
        "NOT VALID SQL;;;",
        float(uc.global_timeout_ms),
    )
    assert status.startswith("error:")
    assert uc.id in logs


@pytest.mark.asyncio
async def test_on_kafka_send_persists_and_saves(isolated_cwd: Path) -> None:
    state = load_default_state()
    uc = state.kafka_use_cases[0]
    producer = AsyncMock()
    producer.start = AsyncMock()
    producer.stop = AsyncMock()

    with patch("aiokafka.AIOKafkaProducer", return_value=producer):
        result = await on_kafka_send(
            state,
            {},
            CLEAN_META,
            uc.bootstrap_url,
            uc.topic,
            uc.hooks,
            uc.messages,
            float(uc.global_timeout_ms),
        )

    assert result[9].startswith("ok:")
    assert Path("configuration.json").exists()


def test_on_clickhouse_send_persists_and_saves(isolated_cwd: Path) -> None:
    state = load_default_state()
    uc = state.clickhouse_use_cases[0]
    client = MagicMock()
    with patch("console.handlers.clickhouse_connect.get_client", return_value=client):
        result = on_clickhouse_send(
            state,
            {},
            CLEAN_META,
            uc.host,
            float(uc.port),
            uc.user,
            uc.password,
            uc.database,
            uc.hooks,
            uc.messages,
            float(uc.global_timeout_ms),
        )
    assert result[12].startswith("ok:")
    assert Path("configuration.json").exists()
