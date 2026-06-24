from console.converters.state_converter import convert_payload, load_default_state
from console.use_case_state import (
    add_clickhouse_use_case,
    add_kafka_use_case,
    apply_kafka_sidebar_value,
    delete_clickhouse_use_case,
    delete_kafka_use_case,
    delete_selected_clickhouse_use_case,
    delete_selected_kafka_use_case,
    rename_selected_clickhouse_use_case,
    rename_selected_kafka_use_case,
    reorder_clickhouse_use_cases,
    reorder_kafka_use_cases,
    select_clickhouse_index,
    select_kafka_index,
    update_clickhouse_use_case_form,
    update_kafka_use_case_form,
)


def test_add_kafka_use_case_selects_new_tab() -> None:
    state = load_default_state()
    state, status = add_kafka_use_case(state)

    assert status == "ok: use case added"
    assert len(state.kafka_use_cases) == 2
    assert state.selected_kafka_index == 1
    assert state.kafka_use_cases[0].id != state.kafka_use_cases[1].id


def test_add_clickhouse_use_case_selects_new_tab() -> None:
    state = load_default_state()
    state, status = add_clickhouse_use_case(state)

    assert status == "ok: use case added"
    assert len(state.clickhouse_use_cases) == 2
    assert state.selected_clickhouse_index == 1
    assert state.clickhouse_use_cases[0].id != state.clickhouse_use_cases[1].id


def test_rename_selected_use_cases() -> None:
    state = load_default_state()
    state, _ = rename_selected_kafka_use_case(state, "Kafka Smoke")
    state, _ = rename_selected_clickhouse_use_case(state, "CH Smoke")

    assert state.kafka_use_cases[0].name == "Kafka Smoke"
    assert state.clickhouse_use_cases[0].name == "CH Smoke"


def test_delete_kafka_use_case_by_id_removes_logs_and_adjusts_selection() -> None:
    state = load_default_state()
    state, _ = add_kafka_use_case(state)
    first_id = state.kafka_use_cases[0].id
    second_id = state.kafka_use_cases[1].id
    logs = {first_id: "first logs", second_id: "second logs"}

    state, logs, status = delete_kafka_use_case(state, first_id, logs)

    assert status == "ok: deleted"
    assert len(state.kafka_use_cases) == 1
    assert state.kafka_use_cases[0].id == second_id
    assert state.selected_kafka_index == 0
    assert first_id not in logs
    assert logs[second_id] == "second logs"


def test_delete_kafka_use_case_removes_logs_and_clamps_index() -> None:
    state = load_default_state()
    state, _ = add_kafka_use_case(state)
    first_id = state.kafka_use_cases[0].id
    second_id = state.kafka_use_cases[1].id
    logs = {first_id: "first logs", second_id: "second logs"}

    state, logs, status = delete_selected_kafka_use_case(state, logs)

    assert status == "ok: deleted"
    assert len(state.kafka_use_cases) == 1
    assert state.selected_kafka_index == 0
    assert second_id not in logs
    assert logs[first_id] == "first logs"


def test_delete_clickhouse_use_case_removes_logs_and_clamps_index() -> None:
    state = load_default_state()
    state, _ = add_clickhouse_use_case(state)
    first_id = state.clickhouse_use_cases[0].id
    second_id = state.clickhouse_use_cases[1].id
    logs = {first_id: "first logs", second_id: "second logs"}

    state, logs, status = delete_selected_clickhouse_use_case(state, logs)

    assert status == "ok: deleted"
    assert len(state.clickhouse_use_cases) == 1
    assert state.selected_clickhouse_index == 0
    assert second_id not in logs
    assert logs[first_id] == "first logs"


def test_select_functions_clamp_indexes() -> None:
    state = load_default_state()
    state = select_kafka_index(state, 99)
    state = select_clickhouse_index(state, 99)

    assert state.selected_kafka_index == 0
    assert state.selected_clickhouse_index == 0


def test_update_form_by_use_case_id_sets_selected_index() -> None:
    state = load_default_state()
    state, _ = add_kafka_use_case(state)
    second = state.kafka_use_cases[1]

    state = update_kafka_use_case_form(
        state,
        second.id,
        "localhost:19092",
        "events-v2",
        "fields = {}",
        '{"event":"x"}',
        800.0,
    )

    assert state.selected_kafka_index == 1
    assert state.kafka_use_cases[1].bootstrap_url == "localhost:19092"
    assert state.kafka_use_cases[1].topic == "events-v2"
    assert state.kafka_use_cases[1].global_timeout_ms == 800


def test_update_clickhouse_form_by_use_case_id_sets_selected_index() -> None:
    state = load_default_state()
    state, _ = add_clickhouse_use_case(state)
    second = state.clickhouse_use_cases[1]

    state = update_clickhouse_use_case_form(
        state,
        second.id,
        "127.0.0.1",
        19000.0,
        "qa",
        "pwd",
        "analytics",
        "fields = {}",
        "SELECT 1",
        900.0,
    )

    assert state.selected_clickhouse_index == 1
    assert state.clickhouse_use_cases[1].host == "127.0.0.1"
    assert state.clickhouse_use_cases[1].port == 19000
    assert state.clickhouse_use_cases[1].database == "analytics"
    assert state.clickhouse_use_cases[1].global_timeout_ms == 900


def test_apply_kafka_sidebar_value_selects_use_case() -> None:
    state = load_default_state()
    state, _ = add_kafka_use_case(state)
    second_id = state.kafka_use_cases[1].id
    sidebar = {
        "items": [{"id": item.id, "name": item.name} for item in state.kafka_use_cases],
        "selected_id": second_id,
    }

    state = apply_kafka_sidebar_value(state, sidebar)

    assert state.selected_kafka_index == 1
    assert state.kafka_use_cases[1].id == second_id


def test_apply_kafka_sidebar_value_deletes_missing_use_case() -> None:
    state = load_default_state()
    state, _ = add_kafka_use_case(state)
    first_id = state.kafka_use_cases[0].id
    second = state.kafka_use_cases[1]
    sidebar = {
        "items": [{"id": second.id, "name": second.name}],
        "selected_id": second.id,
    }

    state = apply_kafka_sidebar_value(state, sidebar)

    assert len(state.kafka_use_cases) == 1
    assert state.kafka_use_cases[0].id == second.id
    assert first_id not in {item.id for item in state.kafka_use_cases}
    assert state.selected_kafka_index == 0


def test_reorder_kafka_use_cases_keeps_selected_use_case() -> None:
    state = load_default_state()
    state, _ = add_kafka_use_case(state)
    selected_id = state.kafka_use_cases[1].id
    state = select_kafka_index(state, 1)
    original_ids = [item.id for item in state.kafka_use_cases]
    reordered_ids = [original_ids[1], original_ids[0]]

    state = reorder_kafka_use_cases(state, reordered_ids)

    assert [item.id for item in state.kafka_use_cases] == reordered_ids
    assert state.selected_kafka_index == 0
    assert state.kafka_use_cases[state.selected_kafka_index].id == selected_id


def test_reorder_clickhouse_use_cases_keeps_selected_use_case() -> None:
    state = load_default_state()
    state, _ = add_clickhouse_use_case(state)
    selected_id = state.clickhouse_use_cases[1].id
    state = select_clickhouse_index(state, 1)
    original_ids = [item.id for item in state.clickhouse_use_cases]
    reordered_ids = [original_ids[1], original_ids[0]]

    state = reorder_clickhouse_use_cases(state, reordered_ids)

    assert [item.id for item in state.clickhouse_use_cases] == reordered_ids
    assert state.selected_clickhouse_index == 0
    assert state.clickhouse_use_cases[state.selected_clickhouse_index].id == selected_id


def test_convert_payload_adds_ids_for_legacy_use_cases() -> None:
    payload = {
        "kafka_use_cases": [{"name": "K1"}],
        "clickhouse_use_cases": [{"name": "C1"}],
        "selected_kafka_index": 0,
        "selected_clickhouse_index": 0,
    }

    converted = convert_payload(payload)
    kafka_rows = converted["kafka_use_cases"]
    clickhouse_rows = converted["clickhouse_use_cases"]

    assert isinstance(kafka_rows, list)
    assert isinstance(clickhouse_rows, list)
    assert kafka_rows[0]["id"] == "kafka-1"
    assert clickhouse_rows[0]["id"] == "clickhouse-1"
