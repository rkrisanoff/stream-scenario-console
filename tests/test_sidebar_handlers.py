import inspect
from pathlib import Path

from console.handlers import (
    on_clickhouse_sidebar_add,
    on_clickhouse_sidebar_change,
    on_kafka_sidebar_add,
    on_kafka_sidebar_change,
)
from console.converters.state_converter import load_default_state
from console.persistence import SaveMeta
from console.use_case_state import (
    add_clickhouse_use_case,
    add_kafka_use_case,
    build_clickhouse_sidebar_value,
    build_kafka_sidebar_value,
    select_clickhouse_index,
    select_kafka_index,
)

CLEAN_SAVE_META = SaveMeta(dirty=False, last_saved_at=None, last_message="")
ALLOW_DELETE = True

KAFKA_SIDEBAR_CHANGE_INPUTS = [
    "sidebar_value",
    "state",
    "logs_by_use_case",
    "save_meta",
    "allow_delete",
    "bootstrap_url",
    "topic",
    "hooks",
    "messages",
    "global_timeout_ms",
]

CLICKHOUSE_SIDEBAR_CHANGE_INPUTS = [
    "sidebar_value",
    "state",
    "logs_by_use_case",
    "save_meta",
    "allow_delete",
    "host",
    "port",
    "user",
    "password",
    "database",
    "hooks",
    "messages",
    "global_timeout_ms",
]


def _update_value(component_update: dict) -> object:
    return component_update["value"]


def test_sidebar_change_handler_parameters_match_gradio_inputs() -> None:
    assert list(inspect.signature(on_kafka_sidebar_change).parameters) == KAFKA_SIDEBAR_CHANGE_INPUTS
    assert list(inspect.signature(on_clickhouse_sidebar_change).parameters) == CLICKHOUSE_SIDEBAR_CHANGE_INPUTS


def test_on_kafka_sidebar_change_accepts_sidebar_dict_first(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    state = load_default_state()
    state, _ = add_kafka_use_case(state)
    first, second = state.kafka_use_cases
    sidebar_value = build_kafka_sidebar_value(state)
    sidebar_value["selected_id"] = second.id

    result = on_kafka_sidebar_change(
        sidebar_value,
        state,
        {},
        CLEAN_SAVE_META,
        ALLOW_DELETE,
        first.bootstrap_url,
        first.topic,
        first.hooks,
        first.messages,
        float(first.global_timeout_ms),
    )

    new_state = result[0]
    assert new_state.selected_kafka_index == 1
    assert _update_value(result[5]) == second.bootstrap_url
    assert _update_value(result[6]) == second.topic


def test_on_kafka_sidebar_change_persists_form_before_switch(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    state = load_default_state()
    state, _ = add_kafka_use_case(state)
    state = select_kafka_index(state, 0)
    first, second = state.kafka_use_cases
    sidebar_value = build_kafka_sidebar_value(state)
    sidebar_value["selected_id"] = second.id

    result = on_kafka_sidebar_change(
        sidebar_value,
        state,
        {},
        CLEAN_SAVE_META,
        ALLOW_DELETE,
        "edited-bootstrap:9092",
        "edited-topic",
        first.hooks,
        first.messages,
        float(first.global_timeout_ms),
    )

    new_state = result[0]
    assert new_state.kafka_use_cases[0].bootstrap_url == "edited-bootstrap:9092"
    assert new_state.kafka_use_cases[0].topic == "edited-topic"
    assert new_state.selected_kafka_index == 1


def test_on_kafka_sidebar_change_renames_via_sidebar_items(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    state = load_default_state()
    sidebar_value = build_kafka_sidebar_value(state)
    sidebar_value["items"][0]["name"] = "Renamed in sidebar"

    result = on_kafka_sidebar_change(
        sidebar_value,
        state,
        {},
        CLEAN_SAVE_META,
        ALLOW_DELETE,
        state.kafka_use_cases[0].bootstrap_url,
        state.kafka_use_cases[0].topic,
        state.kafka_use_cases[0].hooks,
        state.kafka_use_cases[0].messages,
        float(state.kafka_use_cases[0].global_timeout_ms),
    )

    assert result[0].kafka_use_cases[0].name == "Renamed in sidebar"


def test_on_kafka_sidebar_change_deletes_use_case_and_logs(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    state = load_default_state()
    state, _ = add_kafka_use_case(state)
    first, second = state.kafka_use_cases
    sidebar_value = {
        "items": [{"id": second.id, "name": second.name}],
        "selected_id": second.id,
    }

    result = on_kafka_sidebar_change(
        sidebar_value,
        state,
        {first.id: "first logs", second.id: "second logs"},
        CLEAN_SAVE_META,
        ALLOW_DELETE,
        second.bootstrap_url,
        second.topic,
        second.hooks,
        second.messages,
        float(second.global_timeout_ms),
    )

    new_state = result[0]
    logs = result[1]
    assert len(new_state.kafka_use_cases) == 1
    assert new_state.kafka_use_cases[0].id == second.id
    assert first.id not in logs
    assert logs[second.id] == "second logs"


def test_on_clickhouse_sidebar_change_accepts_sidebar_dict_first(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    state = load_default_state()
    state, _ = add_clickhouse_use_case(state)
    first, second = state.clickhouse_use_cases
    sidebar_value = build_clickhouse_sidebar_value(state)
    sidebar_value["selected_id"] = second.id

    result = on_clickhouse_sidebar_change(
        sidebar_value,
        state,
        {},
        CLEAN_SAVE_META,
        ALLOW_DELETE,
        first.host,
        float(first.port),
        first.user,
        first.password,
        first.database,
        first.hooks,
        first.messages,
        float(first.global_timeout_ms),
    )

    new_state = result[0]
    assert new_state.selected_clickhouse_index == 1
    assert _update_value(result[5]) == second.host
    assert _update_value(result[9]) == second.database


def test_on_clickhouse_sidebar_change_persists_form_before_switch(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    state = load_default_state()
    state, _ = add_clickhouse_use_case(state)
    state = select_clickhouse_index(state, 0)
    first, second = state.clickhouse_use_cases
    sidebar_value = build_clickhouse_sidebar_value(state)
    sidebar_value["selected_id"] = second.id

    result = on_clickhouse_sidebar_change(
        sidebar_value,
        state,
        {},
        CLEAN_SAVE_META,
        ALLOW_DELETE,
        "10.0.0.1",
        9440.0,
        first.user,
        first.password,
        "edited-db",
        first.hooks,
        first.messages,
        float(first.global_timeout_ms),
    )

    new_state = result[0]
    assert new_state.clickhouse_use_cases[0].host == "10.0.0.1"
    assert new_state.clickhouse_use_cases[0].port == 9440
    assert new_state.clickhouse_use_cases[0].database == "edited-db"
    assert new_state.selected_clickhouse_index == 1


def test_on_kafka_sidebar_add_creates_new_use_case(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    state = load_default_state()
    use_case = state.kafka_use_cases[0]

    result = on_kafka_sidebar_add(
        state,
        {},
        CLEAN_SAVE_META,
        use_case.bootstrap_url,
        use_case.topic,
        use_case.hooks,
        use_case.messages,
        float(use_case.global_timeout_ms),
    )

    new_state = result[0]
    assert len(new_state.kafka_use_cases) == 2
    assert new_state.selected_kafka_index == 1
    assert result[8] == "ok: use case added"


def test_on_clickhouse_sidebar_add_creates_new_use_case(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    state = load_default_state()
    uc = state.clickhouse_use_cases[0]

    result = on_clickhouse_sidebar_add(
        state,
        {},
        CLEAN_SAVE_META,
        uc.host,
        float(uc.port),
        uc.user,
        uc.password,
        uc.database,
        uc.hooks,
        uc.messages,
        float(uc.global_timeout_ms),
    )

    new_state = result[0]
    assert len(new_state.clickhouse_use_cases) == 2
    assert new_state.selected_clickhouse_index == 1
    assert result[11] == "ok: use case added"


def test_on_kafka_sidebar_change_reorders_and_saves(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    state = load_default_state()
    state, _ = add_kafka_use_case(state)
    ids = [item.id for item in state.kafka_use_cases]
    sidebar = {
        "items": [{"id": ids[1], "name": "B"}, {"id": ids[0], "name": "A"}],
        "selected_id": ids[1],
    }
    uc = state.kafka_use_cases[0]

    result = on_kafka_sidebar_change(
        sidebar,
        state,
        {},
        CLEAN_SAVE_META,
        ALLOW_DELETE,
        uc.bootstrap_url,
        uc.topic,
        uc.hooks,
        uc.messages,
        float(uc.global_timeout_ms),
    )

    new_state = result[0]
    assert [item.id for item in new_state.kafka_use_cases] == [ids[1], ids[0]]
    assert (tmp_path / "configuration.json").exists()


def test_on_clickhouse_sidebar_change_deletes_use_case(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    state = load_default_state()
    state, _ = add_clickhouse_use_case(state)
    first, second = state.clickhouse_use_cases
    sidebar = {"items": [{"id": second.id, "name": second.name}], "selected_id": second.id}

    result = on_clickhouse_sidebar_change(
        sidebar,
        state,
        {first.id: "logs-a", second.id: "logs-b"},
        CLEAN_SAVE_META,
        ALLOW_DELETE,
        second.host,
        float(second.port),
        second.user,
        second.password,
        second.database,
        second.hooks,
        second.messages,
        float(second.global_timeout_ms),
    )

    assert len(result[0].clickhouse_use_cases) == 1
    assert first.id not in result[1]
