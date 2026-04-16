from console.parser import HOOKS_MAX_CODE_CHARS, MessageParser


def test_hooks_full_python_script() -> None:
    parser = MessageParser()
    hooks = (
        "from uuid import uuid4\n"
        "from datetime import datetime, timezone\n"
        "fields['run_id'] = str(uuid4())\n"
        "fields['ts'] = datetime(2026, 1, 1, tzinfo=timezone.utc).isoformat()\n"
        "fields['x'] = 2 + 3 * 10\n"
    )
    result = parser.run_hooks(hooks)
    assert "run_id" in result
    assert result["ts"].startswith("2026-01-01")
    assert result["x"] == "32"


def test_parse_timeout_one_shot_behavior() -> None:
    parser = MessageParser()
    messages = "@timeout 100ms\nA\nB\n@timeout 1s\nC\nD"
    parsed = parser.parse_messages(messages, global_timeout_ms=500, hook_code="")
    assert [item.delay_ms for item in parsed] == [100, 500, 1000, 500]


def test_parse_timeout_seconds_and_plain_number() -> None:
    parser = MessageParser()
    messages = "@timeout 1.5s\nA\n@timeout=250\nB"
    parsed = parser.parse_messages(messages, global_timeout_ms=10, hook_code="")
    assert [item.delay_ms for item in parsed] == [1500, 250]


def test_parse_anchor_mapping() -> None:
    parser = MessageParser()
    hooks = "fields['id'] = 'abc'"
    parsed = parser.parse_messages('{"id":"{{id}}"}', global_timeout_ms=100, hook_code=hooks)
    assert parsed[0].text == '{"id":"abc"}'


def test_parse_anchor_missing_error_message() -> None:
    parser = MessageParser()
    try:
        parser.parse_messages('{"id":"{{missing}}"}', global_timeout_ms=100, hook_code="")
        assert False
    except ValueError as exc:
        assert "anchor not found: missing" in str(exc)


def test_clickhouse_mapping_json_and_sql() -> None:
    parser = MessageParser()
    messages = (
        "@clickhouse.table events\n"
        "@timeout 200ms\n"
        '{"id":"{{id}}"}\n'
        "SELECT 1\n"
    )
    hooks = "fields['id'] = 'u1'"
    actions = parser.parse_clickhouse_messages(messages, global_timeout_ms=500, hook_code=hooks)
    assert actions[0].kind == "json"
    assert actions[0].table_name == "events"
    assert actions[0].delay_ms == 200
    assert actions[0].text == '{"id":"u1"}'
    assert actions[1].kind == "sql"
    assert actions[1].delay_ms == 500


def test_clickhouse_missing_table_error_message() -> None:
    parser = MessageParser()
    try:
        parser.parse_clickhouse_messages('{"id":1}', global_timeout_ms=100, hook_code="")
        assert False
    except ValueError as exc:
        assert "set @clickhouse.table before JSON message" in str(exc)


def test_hooks_fields_must_be_dict_error_message() -> None:
    parser = MessageParser()
    try:
        parser.run_hooks("fields = 42")
        assert False
    except ValueError as exc:
        assert "hooks: fields must be dict" in str(exc)


def test_hooks_import_os_is_forbidden() -> None:
    parser = MessageParser()
    try:
        parser.run_hooks("import os\nfields['x'] = 1")
        assert False
    except ValueError as exc:
        assert "forbidden system operation detected" in str(exc)


def test_hooks_code_size_limit() -> None:
    parser = MessageParser()
    too_large = "x" * (HOOKS_MAX_CODE_CHARS + 1)
    try:
        parser.run_hooks(too_large)
        assert False
    except ValueError as exc:
        assert "code is too large" in str(exc)


def test_hooks_execution_timeout() -> None:
    parser = MessageParser()
    try:
        parser.run_hooks("while True:\n    pass")
        assert False
    except ValueError as exc:
        assert "execution timeout" in str(exc)
