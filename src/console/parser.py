from dataclasses import dataclass
from enum import StrEnum
import json
import multiprocessing as mp
import re
from uuid import uuid4


class Directive(StrEnum):
    TIMEOUT = "@timeout"
    CLICKHOUSE_TABLE = "@clickhouse.table"


DIRECTIVE_TIMEOUT = Directive.TIMEOUT
DIRECTIVE_CLICKHOUSE_TABLE = Directive.CLICKHOUSE_TABLE

class LineKind(StrEnum):
    MESSAGE = "message"
    TIMEOUT = "timeout"
    CLICKHOUSE_TABLE = "clickhouse_table"


class ClickHouseActionKind(StrEnum):
    JSON = "json"
    SQL = "sql"

HOOKS_FORBIDDEN_TOKENS = (
    "import os",
    "from os",
    "__import__(",
    "import subprocess",
    "from subprocess",
    "import socket",
    "from socket",
    "import shutil",
    "from shutil",
    "import pathlib",
    "from pathlib",
    "import importlib",
    "from importlib",
    "import sys",
    "from sys",
    "sys.",
    "open(",
)
HOOKS_FORBIDDEN_ERROR_MESSAGE = (
    "hooks: forbidden system operation detected; "
    "imports/os access/process/network/filesystem are not allowed"
)
HOOKS_MAX_CODE_CHARS = 8000
HOOKS_MAX_EXECUTION_SECONDS = 2.0
HOOKS_MAX_FIELDS = 128
HOOKS_MAX_KEY_CHARS = 128
HOOKS_MAX_VALUE_CHARS = 4096
HOOKS_TOO_LARGE_ERROR_MESSAGE = f"hooks: code is too large; max {HOOKS_MAX_CODE_CHARS} chars"
HOOKS_TIMEOUT_ERROR_MESSAGE = f"hooks: execution timeout; max {HOOKS_MAX_EXECUTION_SECONDS} seconds"

type JsonPrimitive = str | int | float | bool | None
type JsonValue = JsonPrimitive | list["JsonValue"] | dict[str, "JsonValue"]


def _convert_fields_with_limits(raw_fields: JsonValue) -> dict[str, str]:
    if not isinstance(raw_fields, dict):
        raise ValueError("hooks: fields must be dict")
    if len(raw_fields) > HOOKS_MAX_FIELDS:
        raise ValueError(f"hooks: too many fields; max {HOOKS_MAX_FIELDS}")
    result: dict[str, str] = {}
    for key, value in raw_fields.items():
        if not isinstance(key, str):
            raise ValueError("hooks: fields keys must be strings")
        if len(key) > HOOKS_MAX_KEY_CHARS:
            raise ValueError(f"hooks: field key is too long; max {HOOKS_MAX_KEY_CHARS}")
        value_as_text = str(value)
        if len(value_as_text) > HOOKS_MAX_VALUE_CHARS:
            raise ValueError(f"hooks: field value is too long; max {HOOKS_MAX_VALUE_CHARS}")
        result[key] = value_as_text
    return result


def _run_hooks_worker(hook_code: str, result_queue: mp.Queue) -> None:
    try:
        scope = {
            "__builtins__": __builtins__,
            "uuid4": uuid4,
            "fields": {},
        }
        exec(hook_code, scope, scope)
        result = _convert_fields_with_limits(scope.get("fields", {}))
        result_queue.put(("ok", result))
    except Exception as exc:
        result_queue.put(("error", str(exc)))


@dataclass(slots=True)
class ParsedMessage:
    delay_ms: int
    text: str


@dataclass(slots=True)
class ParsedClickHouseAction:
    kind: ClickHouseActionKind
    table_name: str
    text: str
    delay_ms: int


@dataclass(slots=True)
class ParsedLine:
    kind: LineKind
    payload: str


class MessageParser:
    _anchor_pattern = re.compile(r"\{\{\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*\}\}")

    def run_hooks(self, hook_code: str) -> dict[str, str]:
        if not hook_code.strip():
            return {}
        if len(hook_code) > HOOKS_MAX_CODE_CHARS:
            raise ValueError(HOOKS_TOO_LARGE_ERROR_MESSAGE)
        lowered = hook_code.lower()
        for token in HOOKS_FORBIDDEN_TOKENS:
            if token in lowered:
                raise ValueError(HOOKS_FORBIDDEN_ERROR_MESSAGE)
        mp_context = mp.get_context("fork")
        result_queue: mp.Queue = mp_context.Queue(maxsize=1)
        process = mp_context.Process(target=_run_hooks_worker, args=(hook_code, result_queue), daemon=True)
        process.start()
        process.join(HOOKS_MAX_EXECUTION_SECONDS)
        if process.is_alive():
            process.terminate()
            process.join()
            raise ValueError(HOOKS_TIMEOUT_ERROR_MESSAGE)
        if result_queue.empty():
            raise ValueError("hooks: execution failed")
        status, payload = result_queue.get_nowait()
        if status == "error":
            raise ValueError(f"hooks: execution failed: {payload}")
        return payload

    def parse_lines(self, messages_text: str) -> list[ParsedLine]:
        result: list[ParsedLine] = []
        for raw_line in messages_text.splitlines():
            line = raw_line.strip()
            if not line:
                continue
            if line.startswith(DIRECTIVE_TIMEOUT):
                result.append(ParsedLine(kind=LineKind.TIMEOUT, payload=self._parse_directive_payload(line)))
                continue
            if line.startswith(DIRECTIVE_CLICKHOUSE_TABLE):
                result.append(ParsedLine(kind=LineKind.CLICKHOUSE_TABLE, payload=self._parse_directive_payload(line)))
                continue
            result.append(ParsedLine(kind=LineKind.MESSAGE, payload=line))
        return result

    def parse_messages(
        self,
        messages_text: str,
        global_timeout_ms: int,
        hook_code: str = "",
    ) -> list[ParsedMessage]:
        anchors = self.run_hooks(hook_code)
        rows = self.parse_lines(messages_text)
        result: list[ParsedMessage] = []
        global_delay = int(global_timeout_ms)
        next_timeout: int | None = None
        for row in rows:
            if row.kind == LineKind.TIMEOUT:
                next_timeout = self._parse_duration_to_ms(row.payload)
                continue
            if row.kind == LineKind.CLICKHOUSE_TABLE:
                continue
            text = self._replace_anchors(row.payload, anchors)
            timeout_ms = global_delay if next_timeout is None else next_timeout
            result.append(ParsedMessage(delay_ms=timeout_ms, text=text))
            next_timeout = None
        return result

    def parse_clickhouse_messages(
        self,
        messages_text: str,
        global_timeout_ms: int,
        hook_code: str = "",
    ) -> list[ParsedClickHouseAction]:
        anchors = self.run_hooks(hook_code)
        rows = self.parse_lines(messages_text)
        result: list[ParsedClickHouseAction] = []
        current_table = ""
        global_delay = int(global_timeout_ms)
        next_timeout: int | None = None
        for row in rows:
            if row.kind == LineKind.TIMEOUT:
                next_timeout = self._parse_duration_to_ms(row.payload)
                continue
            if row.kind == LineKind.CLICKHOUSE_TABLE:
                current_table = row.payload
                continue
            timeout_ms = global_delay if next_timeout is None else next_timeout
            next_timeout = None
            text = self._replace_anchors(row.payload, anchors).strip()
            try:
                decoded = json.loads(text)
                is_json_object = isinstance(decoded, dict)
            except Exception:
                is_json_object = False
            if is_json_object:
                if not current_table:
                    raise ValueError("set @clickhouse.table before JSON message")
                result.append(
                    ParsedClickHouseAction(
                        kind=ClickHouseActionKind.JSON,
                        table_name=current_table,
                        text=text,
                        delay_ms=timeout_ms,
                    )
                )
            else:
                result.append(
                    ParsedClickHouseAction(
                        kind=ClickHouseActionKind.SQL,
                        table_name="",
                        text=text,
                        delay_ms=timeout_ms,
                    )
                )
        return result

    def _replace_anchors(self, line: str, anchors: dict[str, str]) -> str:
        def replace(match: re.Match[str]) -> str:
            key = match.group(1)
            if key not in anchors:
                raise ValueError(f"anchor not found: {key}")
            return anchors[key]

        return self._anchor_pattern.sub(replace, line)

    def _parse_directive_payload(self, line: str) -> str:
        if " " in line:
            payload = line.split(" ", maxsplit=1)[1].strip()
        elif "=" in line:
            payload = line.split("=", maxsplit=1)[1].strip()
        else:
            payload = ""
        if not payload:
            if line.startswith(DIRECTIVE_TIMEOUT):
                raise ValueError("directive @timeout requires value")
            raise ValueError("directive @clickhouse.table requires value")
        return payload

    def _parse_duration_to_ms(self, value: str) -> int:
        raw = value.strip().lower()
        if raw.endswith("ms"):
            parsed = int(raw[:-2].strip())
        elif raw.endswith("s"):
            parsed = int(float(raw[:-1].strip()) * 1000)
        else:
            parsed = int(raw)
        if parsed < 0:
            raise ValueError("timeout must be >= 0")
        return parsed
