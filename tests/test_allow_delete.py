from console.converters.state_converter import load_default_state
from console.handlers import guard_sidebar_deletion, on_kafka_sidebar_change
from console.persistence import SaveMeta
from console.use_case_state import add_kafka_use_case, build_kafka_sidebar_value


def test_guard_sidebar_deletion_blocks_missing_items() -> None:
    state = load_default_state()
    state, _ = add_kafka_use_case(state)
    first, second = state.kafka_use_cases
    sidebar = {"items": [{"id": second.id, "name": second.name}], "selected_id": second.id}

    guarded = guard_sidebar_deletion(
        state,
        sidebar,
        allow_delete=False,
        cases_attr="kafka_use_cases",
        build_sidebar_value=build_kafka_sidebar_value,
    )

    assert len(guarded["items"]) == 2


def test_guard_sidebar_deletion_allows_when_enabled() -> None:
    state = load_default_state()
    state, _ = add_kafka_use_case(state)
    second = state.kafka_use_cases[1]
    sidebar = {"items": [{"id": second.id, "name": second.name}], "selected_id": second.id}

    guarded = guard_sidebar_deletion(
        state,
        sidebar,
        allow_delete=True,
        cases_attr="kafka_use_cases",
        build_sidebar_value=build_kafka_sidebar_value,
    )

    assert len(guarded["items"]) == 1


def test_on_kafka_sidebar_change_ignores_delete_when_disabled(tmp_path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    state = load_default_state()
    state, _ = add_kafka_use_case(state)
    first, second = state.kafka_use_cases
    sidebar = {"items": [{"id": second.id, "name": second.name}], "selected_id": second.id}

    result = on_kafka_sidebar_change(
        sidebar,
        state,
        {},
        SaveMeta(dirty=False, last_saved_at=None, last_message=""),
        False,
        second.bootstrap_url,
        second.topic,
        second.hooks,
        second.messages,
        float(second.global_timeout_ms),
    )

    assert len(result[0].kafka_use_cases) == 2
