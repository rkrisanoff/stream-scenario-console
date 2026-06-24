import pytest

from console.converters.state_converter import load_default_state
from console.handlers import on_clickhouse_field_change, on_kafka_field_change
from console.persistence import SaveMeta, mark_dirty


CLEAN_META = SaveMeta(dirty=False, last_saved_at=None, last_message="loaded")


@pytest.fixture
def kafka_form():
    state = load_default_state()
    uc = state.kafka_use_cases[0]
    return state, uc.bootstrap_url, uc.topic, uc.hooks, uc.messages, float(uc.global_timeout_ms)


@pytest.fixture
def clickhouse_form():
    state = load_default_state()
    uc = state.clickhouse_use_cases[0]
    return (
        state,
        uc.host,
        float(uc.port),
        uc.user,
        uc.password,
        uc.database,
        uc.hooks,
        uc.messages,
        float(uc.global_timeout_ms),
    )


def test_kafka_field_change_marks_dirty(kafka_form) -> None:
    state, url, topic, hooks, messages, timeout = kafka_form
    new_state, meta, status = on_kafka_field_change(state, CLEAN_META, url, topic, hooks, messages, timeout)
    assert meta.dirty is True
    assert status == "● Несохранённые изменения"
    assert new_state.kafka_use_cases[0].topic == topic


def test_kafka_field_change_updates_bootstrap_url(kafka_form) -> None:
    state, _, topic, hooks, messages, timeout = kafka_form
    new_state, _, _ = on_kafka_field_change(state, CLEAN_META, "broker:9092", topic, hooks, messages, timeout)
    assert new_state.kafka_use_cases[0].bootstrap_url == "broker:9092"


def test_kafka_field_change_updates_topic(kafka_form) -> None:
    state, url, _, hooks, messages, timeout = kafka_form
    new_state, _, _ = on_kafka_field_change(state, CLEAN_META, url, "events-new", hooks, messages, timeout)
    assert new_state.kafka_use_cases[0].topic == "events-new"


def test_kafka_field_change_updates_hooks(kafka_form) -> None:
    state, url, topic, _, messages, timeout = kafka_form
    new_state, _, _ = on_kafka_field_change(state, CLEAN_META, url, topic, "fields = {'x': 1}", messages, timeout)
    assert new_state.kafka_use_cases[0].hooks == "fields = {'x': 1}"


def test_kafka_field_change_updates_messages(kafka_form) -> None:
    state, url, topic, hooks, _, timeout = kafka_form
    new_state, _, _ = on_kafka_field_change(state, CLEAN_META, url, topic, hooks, '{"a":1}', timeout)
    assert new_state.kafka_use_cases[0].messages == '{"a":1}'


def test_kafka_field_change_updates_timeout(kafka_form) -> None:
    state, url, topic, hooks, messages, _ = kafka_form
    new_state, _, _ = on_kafka_field_change(state, CLEAN_META, url, topic, hooks, messages, 1500.0)
    assert new_state.kafka_use_cases[0].global_timeout_ms == 1500


def test_kafka_field_change_preserves_dirty_flag(kafka_form) -> None:
    state, url, topic, hooks, messages, timeout = kafka_form
    dirty = mark_dirty(CLEAN_META)
    _, meta, _ = on_kafka_field_change(state, dirty, url, topic, hooks, messages, timeout)
    assert meta.dirty is True


def test_clickhouse_field_change_marks_dirty(clickhouse_form) -> None:
    state, host, port, user, password, database, hooks, messages, timeout = clickhouse_form
    new_state, meta, status = on_clickhouse_field_change(
        state, CLEAN_META, host, port, user, password, database, hooks, messages, timeout
    )
    assert meta.dirty is True
    assert status == "● Несохранённые изменения"
    assert new_state.clickhouse_use_cases[0].host == host


def test_clickhouse_field_change_updates_host(clickhouse_form) -> None:
    state, _, port, user, password, database, hooks, messages, timeout = clickhouse_form
    new_state, _, _ = on_clickhouse_field_change(
        state, CLEAN_META, "10.0.0.5", port, user, password, database, hooks, messages, timeout
    )
    assert new_state.clickhouse_use_cases[0].host == "10.0.0.5"


def test_clickhouse_field_change_updates_port(clickhouse_form) -> None:
    state, host, _, user, password, database, hooks, messages, timeout = clickhouse_form
    new_state, _, _ = on_clickhouse_field_change(
        state, CLEAN_META, host, 9440.0, user, password, database, hooks, messages, timeout
    )
    assert new_state.clickhouse_use_cases[0].port == 9440


def test_clickhouse_field_change_updates_database(clickhouse_form) -> None:
    state, host, port, user, password, _, hooks, messages, timeout = clickhouse_form
    new_state, _, _ = on_clickhouse_field_change(
        state, CLEAN_META, host, port, user, password, "warehouse", hooks, messages, timeout
    )
    assert new_state.clickhouse_use_cases[0].database == "warehouse"


def test_clickhouse_field_change_updates_password(clickhouse_form) -> None:
    state, host, port, user, _, database, hooks, messages, timeout = clickhouse_form
    new_state, _, _ = on_clickhouse_field_change(
        state, CLEAN_META, host, port, user, "secret", database, hooks, messages, timeout
    )
    assert new_state.clickhouse_use_cases[0].password == "secret"


def test_clickhouse_field_change_strips_host(clickhouse_form) -> None:
    state, _, port, user, password, database, hooks, messages, timeout = clickhouse_form
    new_state, _, _ = on_clickhouse_field_change(
        state, CLEAN_META, "  ch.local  ", port, user, password, database, hooks, messages, timeout
    )
    assert new_state.clickhouse_use_cases[0].host == "ch.local"
