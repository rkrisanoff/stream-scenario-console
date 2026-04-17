import asyncio

from console.parser import ClickHouseActionKind, ParsedClickHouseAction, ParsedMessage
from console.runner import CommandKind, ScenarioRunner


class FakeProducer:
    def __init__(self) -> None:
        self.calls: list[tuple[str, bytes]] = []

    async def send_and_wait(self, topic: str, payload: bytes) -> None:
        self.calls.append((topic, payload))


class FakeClickHouseClient:
    def __init__(self) -> None:
        self.inserts: list[tuple[str, list[list[str]], list[str]]] = []
        self.commands: list[str] = []

    def insert(self, table_name: str, rows: list[list[str]], column_names: list[str]) -> None:
        self.inserts.append((table_name, rows, column_names))

    def command(self, sql: str) -> None:
        self.commands.append(sql)


def test_build_kafka_commands_has_sleep_between_messages() -> None:
    runner = ScenarioRunner()
    parsed = [
        ParsedMessage(delay_ms=100, text='{"event":"a"}'),
        ParsedMessage(delay_ms=200, text='{"event":"b"}'),
    ]
    commands = runner.build_kafka_commands(parsed, "events")
    assert [command.kind for command in commands] == [
        CommandKind.KAFKA_SEND,
        CommandKind.SLEEP,
        CommandKind.KAFKA_SEND,
    ]


def test_run_kafka_commands_emits_logs_and_sends_messages() -> None:
    logs: list[str] = []
    sleeps: list[float] = []

    async def fake_async_sleep(seconds: float) -> None:
        sleeps.append(seconds)

    runner = ScenarioRunner(log_handler=logs.append, async_sleep=fake_async_sleep)
    parsed = [
        ParsedMessage(delay_ms=120, text='{"event":"open"}'),
        ParsedMessage(delay_ms=250, text='{"event":"click"}'),
    ]
    commands = runner.build_kafka_commands(parsed, "events")
    producer = FakeProducer()
    asyncio.run(runner.run_kafka_commands(producer, commands))

    assert len(producer.calls) == 2
    assert producer.calls[0][0] == "events"
    assert sleeps == [0.12]
    assert any("kafka send" in line for line in logs)
    assert any("sleep -> 120 ms" in line for line in logs)


def test_build_and_run_clickhouse_commands() -> None:
    logs: list[str] = []
    sleeps: list[float] = []
    runner = ScenarioRunner(log_handler=logs.append, sync_sleep=sleeps.append)
    actions = [
        ParsedClickHouseAction(kind=ClickHouseActionKind.JSON, table_name="events", text='{"event":"open"}', delay_ms=200),
        ParsedClickHouseAction(kind=ClickHouseActionKind.SQL, table_name="", text="SELECT 1", delay_ms=500),
    ]
    commands = runner.build_clickhouse_commands(actions)
    client = FakeClickHouseClient()
    runner.run_clickhouse_commands(client, commands)

    assert len(client.inserts) == 1
    assert len(client.commands) == 1
    assert sleeps == [0.2]
    assert any("clickhouse insert" in line for line in logs)
    assert any("clickhouse sql" in line for line in logs)
