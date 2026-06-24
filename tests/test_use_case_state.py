import json
from pathlib import Path

import pytest

from console.converters.state_converter import (
    convert_int,
    convert_loaded_state,
    convert_payload,
    dump_state,
    dump_state_to_json,
    load_default_state,
    load_state,
    load_state_from_json,
)
from console.dtos import AppStateDto
from console.use_case_state import (
    apply_clickhouse_sidebar_value,
    build_clickhouse_sidebar_value,
    build_kafka_sidebar_value,
    clamp_index,
    clone_state,
    persist_clickhouse_form,
    persist_kafka_form,
    rename_selected_kafka_use_case,
)


def test_convert_int_valid() -> None:
    assert convert_int("42", 0) == 42
    assert convert_int(3.9, 0) == 3


def test_convert_int_fallback() -> None:
    assert convert_int("bad", 99) == 99
    assert convert_int(None, 7) == 7


def test_convert_payload_fills_kafka_defaults() -> None:
    payload = {"kafka_use_cases": [{"name": "K"}]}
    result = convert_payload(payload)
    row = result["kafka_use_cases"][0]
    assert row["id"] == "kafka-1"
    assert row["name"] == "K"
    assert "bootstrap_url" in row
    assert "topic" in row


def test_convert_payload_fills_clickhouse_defaults() -> None:
    payload = {"clickhouse_use_cases": [{"name": "C"}]}
    result = convert_payload(payload)
    row = result["clickhouse_use_cases"][0]
    assert row["id"] == "clickhouse-1"
    assert "host" in row
    assert "port" in row


def test_convert_payload_clamps_indexes() -> None:
    payload = {"selected_kafka_index": "5", "selected_clickhouse_index": "x"}
    result = convert_payload(payload)
    assert result["selected_kafka_index"] == 5
    assert result["selected_clickhouse_index"] == 0


def test_load_state_from_json_invalid_returns_default() -> None:
    state = load_state_from_json("{broken")
    assert len(state.kafka_use_cases) == 1
    assert len(state.clickhouse_use_cases) == 1


def test_load_state_from_json_non_dict_returns_default() -> None:
    state = load_state_from_json("[1,2,3]")
    assert isinstance(state, AppStateDto)


def test_convert_loaded_state_clamps_out_of_range_indexes() -> None:
    state = load_default_state()
    state.selected_kafka_index = 99
    state.selected_clickhouse_index = -5
    fixed = convert_loaded_state(state)
    assert fixed.selected_kafka_index == 0
    assert fixed.selected_clickhouse_index == 0


def test_convert_loaded_state_adds_missing_use_cases() -> None:
    state = AppStateDto(kafka_use_cases=[], clickhouse_use_cases=[], selected_kafka_index=0, selected_clickhouse_index=0)
    fixed = convert_loaded_state(state)
    assert len(fixed.kafka_use_cases) == 1
    assert len(fixed.clickhouse_use_cases) == 1


def test_dump_and_load_roundtrip(tmp_path: Path) -> None:
    original = load_default_state()
    original.kafka_use_cases[0].topic = "roundtrip-topic"
    path = tmp_path / "configuration.json"
    dump_state(path, original)
    loaded = load_state(path)
    assert loaded.kafka_use_cases[0].topic == "roundtrip-topic"
    assert loaded.kafka_use_cases[0].id == original.kafka_use_cases[0].id


def test_dump_state_to_json_is_valid_json() -> None:
    payload = json.loads(dump_state_to_json(load_default_state()))
    assert "kafka_use_cases" in payload


def test_load_state_missing_file_returns_default(tmp_path: Path) -> None:
    state = load_state(tmp_path / "missing.json")
    assert len(state.kafka_use_cases) == 1


def test_clamp_index_bounds() -> None:
    assert clamp_index(5, 3) == 2
    assert clamp_index(-1, 3) == 0
    assert clamp_index(0, 0) == 0


def test_clone_state_is_independent() -> None:
    state = load_default_state()
    cloned = clone_state(state)
    cloned.kafka_use_cases[0].topic = "changed"
    assert state.kafka_use_cases[0].topic != "changed"


def test_build_kafka_sidebar_value_shape() -> None:
    state = load_default_state()
    value = build_kafka_sidebar_value(state)
    assert value["selected_id"] == state.kafka_use_cases[0].id
    assert len(value["items"]) == 1
    assert value["items"][0]["id"] == state.kafka_use_cases[0].id


def test_build_clickhouse_sidebar_value_shape() -> None:
    state = load_default_state()
    value = build_clickhouse_sidebar_value(state)
    assert value["selected_id"] == state.clickhouse_use_cases[0].id


def test_persist_kafka_form_updates_selected() -> None:
    state = load_default_state()
    uc = state.kafka_use_cases[0]
    new_state = persist_kafka_form(state, "host:9092", "t1", uc.hooks, uc.messages, 500.0)
    assert new_state.kafka_use_cases[0].bootstrap_url == "host:9092"
    assert new_state.kafka_use_cases[0].topic == "t1"
    assert new_state.kafka_use_cases[0].global_timeout_ms == 500


def test_persist_clickhouse_form_updates_selected() -> None:
    state = load_default_state()
    uc = state.clickhouse_use_cases[0]
    new_state = persist_clickhouse_form(state, "h", 9000.0, "u", "p", "db", uc.hooks, uc.messages, 600.0)
    assert new_state.clickhouse_use_cases[0].host == "h"
    assert new_state.clickhouse_use_cases[0].port == 9000


def test_apply_clickhouse_sidebar_value_reorders() -> None:
    state = load_default_state()
    from console.use_case_state import add_clickhouse_use_case

    state, _ = add_clickhouse_use_case(state)
    ids = [item.id for item in state.clickhouse_use_cases]
    sidebar = {
        "items": [{"id": ids[1], "name": "Second"}, {"id": ids[0], "name": "First"}],
        "selected_id": ids[1],
    }
    result = apply_clickhouse_sidebar_value(state, sidebar)
    assert [item.id for item in result.clickhouse_use_cases] == [ids[1], ids[0]]
    assert result.selected_clickhouse_index == 0


def test_apply_clickhouse_sidebar_value_rejects_unknown_id() -> None:
    state = load_default_state()
    sidebar = {"items": [{"id": "unknown", "name": "X"}], "selected_id": "unknown"}
    result = apply_clickhouse_sidebar_value(state, sidebar)
    assert result.kafka_use_cases[0].id == state.kafka_use_cases[0].id


def test_rename_empty_name_returns_error() -> None:
    state = load_default_state()
    new_state, status = rename_selected_kafka_use_case(state, "   ")
    assert new_state is state
    assert status == "error: empty use case name"
