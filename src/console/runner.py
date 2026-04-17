import asyncio
from dataclasses import dataclass
from enum import StrEnum
from collections.abc import Awaitable, Callable
import json
import time

from console.parser import ClickHouseActionKind, ParsedClickHouseAction, ParsedMessage


type JsonPrimitive = str | int | float | bool | None
type JsonValue = JsonPrimitive | list["JsonValue"] | dict[str, "JsonValue"]
type LogHandler = Callable[[str], None]
type AsyncSleep = Callable[[float], Awaitable[None]]
type SyncSleep = Callable[[float], None]


class CommandKind(StrEnum):
    KAFKA_SEND = "kafka_send"
    CLICKHOUSE_INSERT_JSON = "clickhouse_insert_json"
    CLICKHOUSE_SQL = "clickhouse_sql"
    SLEEP = "sleep"


@dataclass(slots=True)
class KafkaSendCommand:
    kind: CommandKind
    topic: str
    text: str


@dataclass(slots=True)
class ClickHouseInsertJsonCommand:
    kind: CommandKind
    table_name: str
    row: dict[str, JsonValue]


@dataclass(slots=True)
class ClickHouseSqlCommand:
    kind: CommandKind
    sql: str


@dataclass(slots=True)
class SleepCommand:
    kind: CommandKind
    delay_ms: int


type RunnerCommand = KafkaSendCommand | ClickHouseInsertJsonCommand | ClickHouseSqlCommand | SleepCommand


class ScenarioRunner:
    def __init__(
        self,
        log_handler: LogHandler | None = None,
        async_sleep: AsyncSleep | None = None,
        sync_sleep: SyncSleep | None = None,
    ) -> None:
        self._log_handler = log_handler
        self._async_sleep = async_sleep or asyncio.sleep
        self._sync_sleep = sync_sleep or time.sleep

    def emit_log(self, line: str) -> None:
        if self._log_handler is not None:
            self._log_handler(line)

    def build_kafka_commands(self, parsed_messages: list[ParsedMessage], topic: str) -> list[RunnerCommand]:
        commands: list[RunnerCommand] = []
        for index, item in enumerate(parsed_messages):
            commands.append(KafkaSendCommand(kind=CommandKind.KAFKA_SEND, topic=topic, text=item.text))
            if index < len(parsed_messages) - 1:
                commands.append(SleepCommand(kind=CommandKind.SLEEP, delay_ms=item.delay_ms))
        return commands

    def build_clickhouse_commands(self, parsed_actions: list[ParsedClickHouseAction]) -> list[RunnerCommand]:
        commands: list[RunnerCommand] = []
        for index, action in enumerate(parsed_actions):
            if action.kind == ClickHouseActionKind.JSON:
                decoded = json.loads(action.text)
                if not isinstance(decoded, dict):
                    raise ValueError("json message must be object")
                commands.append(
                    ClickHouseInsertJsonCommand(
                        kind=CommandKind.CLICKHOUSE_INSERT_JSON,
                        table_name=action.table_name,
                        row=decoded,
                    )
                )
            else:
                commands.append(ClickHouseSqlCommand(kind=CommandKind.CLICKHOUSE_SQL, sql=action.text))
            if index < len(parsed_actions) - 1:
                commands.append(SleepCommand(kind=CommandKind.SLEEP, delay_ms=action.delay_ms))
        return commands

    async def run_kafka_commands(self, producer, commands: list[RunnerCommand]) -> None:
        for command in commands:
            if command.kind == CommandKind.KAFKA_SEND:
                assert isinstance(command, KafkaSendCommand)
                self.emit_log(f"kafka send -> topic={command.topic}, bytes={len(command.text.encode('utf-8'))}")
                await producer.send_and_wait(command.topic, command.text.encode("utf-8"))
            elif command.kind == CommandKind.SLEEP:
                assert isinstance(command, SleepCommand)
                self.emit_log(f"sleep -> {command.delay_ms} ms")
                await self._async_sleep(command.delay_ms / 1000)
            else:
                raise ValueError(f"unsupported kafka command: {command.kind}")

    def run_clickhouse_commands(self, client, commands: list[RunnerCommand]) -> None:
        for command in commands:
            if command.kind == CommandKind.CLICKHOUSE_INSERT_JSON:
                assert isinstance(command, ClickHouseInsertJsonCommand)
                self.emit_log(f"clickhouse insert -> table={command.table_name}, cols={len(command.row.keys())}")
                client.insert(command.table_name, [list(command.row.values())], column_names=list(command.row.keys()))
            elif command.kind == CommandKind.CLICKHOUSE_SQL:
                assert isinstance(command, ClickHouseSqlCommand)
                self.emit_log(f"clickhouse sql -> {command.sql[:80]}")
                client.command(command.sql)
            elif command.kind == CommandKind.SLEEP:
                assert isinstance(command, SleepCommand)
                self.emit_log(f"sleep -> {command.delay_ms} ms")
                self._sync_sleep(command.delay_ms / 1000)
            else:
                raise ValueError(f"unsupported clickhouse command: {command.kind}")
