DEFAULT_KAFKA_BOOTSTRAP_URL = "localhost:9092"
DEFAULT_KAFKA_TOPIC = "events"
DEFAULT_CLICKHOUSE_ENDPOINT = "localhost:8123"
DEFAULT_GLOBAL_TIMEOUT_MS = 500

EXAMPLE_KAFKA_HOOKS = (
    "from uuid import uuid4\n"
    "from datetime import datetime, timezone\n"
    "fields['run_id'] = str(uuid4())\n"
    "fields['ts'] = datetime.now(timezone.utc).isoformat()"
)

EXAMPLE_KAFKA_MESSAGES = (
    "@timeout 120ms\n"
    '{"event":"open","run_id":"{{run_id}}","ts":"{{ts}}"}\n'
    '{"event":"click","run_id":"{{run_id}}","ts":"{{ts}}"}'
)

EXAMPLE_CLICKHOUSE_HOOKS = EXAMPLE_KAFKA_HOOKS

EXAMPLE_CLICKHOUSE_MESSAGES = (
    "@clickhouse.table events\n"
    "@timeout 300ms\n"
    '{"event":"open","run_id":"{{run_id}}","ts":"{{ts}}"}\n'
    "OPTIMIZE TABLE events FINAL"
)


def build_main_help_columns(timeout_directive: str, clickhouse_table_directive: str) -> tuple[str, str]:
    kafka_messages = EXAMPLE_KAFKA_MESSAGES.replace("@timeout", timeout_directive)
    clickhouse_messages = EXAMPLE_CLICKHOUSE_MESSAGES.replace("@clickhouse.table", clickhouse_table_directive).replace(
        "@timeout", timeout_directive
    )
    left = (
        "### Общая справка\n"
        f"- `{timeout_directive}` влияет только на следующую строку.\n"
        f"- `{clickhouse_table_directive}` нужен перед JSON для ClickHouse.\n"
        "- Hooks: обычный Python script, результат через `fields`.\n"
        "- Якоря: `{{name}}`.\n"
        "- `import os` и системные операции запрещены.\n\n"
        "Пример hooks:\n"
        "```python\n"
        f"{EXAMPLE_KAFKA_HOOKS}\n"
        "```"
    )
    right = (
        "### Факты и примеры\n"
        "- Kafka: строки отправляются как сообщения.\n"
        "- ClickHouse: JSON object -> insert, остальное -> SQL.\n\n"
        "Пример Kafka:\n"
        "```text\n"
        f"{kafka_messages}\n"
        "```\n\n"
        "Пример ClickHouse:\n"
        "```text\n"
        f"{clickhouse_messages}\n"
        "```"
    )
    return left, right
