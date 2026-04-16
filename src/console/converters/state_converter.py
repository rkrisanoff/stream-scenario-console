from dataclasses import asdict
import json
from pathlib import Path

from adaptix import Retort

from console.dtos import AppStateDto, ClickHouseUseCaseDto, KafkaUseCaseDto
from console.help_texts import (
    DEFAULT_CLICKHOUSE_ENDPOINT,
    DEFAULT_GLOBAL_TIMEOUT_MS,
    DEFAULT_KAFKA_BOOTSTRAP_URL,
    DEFAULT_KAFKA_TOPIC,
    EXAMPLE_CLICKHOUSE_HOOKS,
    EXAMPLE_CLICKHOUSE_MESSAGES,
    EXAMPLE_KAFKA_HOOKS,
    EXAMPLE_KAFKA_MESSAGES,
)


RETORT_STATE = Retort()


def load_default_kafka_use_case(name: str) -> KafkaUseCaseDto:
    return KafkaUseCaseDto(
        name=name,
        bootstrap_url=DEFAULT_KAFKA_BOOTSTRAP_URL,
        topic=DEFAULT_KAFKA_TOPIC,
        hooks=EXAMPLE_KAFKA_HOOKS,
        messages=EXAMPLE_KAFKA_MESSAGES,
        global_timeout_ms=DEFAULT_GLOBAL_TIMEOUT_MS,
    )


def load_default_clickhouse_use_case(name: str) -> ClickHouseUseCaseDto:
    return ClickHouseUseCaseDto(
        name=name,
        endpoint=DEFAULT_CLICKHOUSE_ENDPOINT,
        hooks=EXAMPLE_CLICKHOUSE_HOOKS,
        messages=EXAMPLE_CLICKHOUSE_MESSAGES,
        global_timeout_ms=DEFAULT_GLOBAL_TIMEOUT_MS,
    )


def load_default_state() -> AppStateDto:
    return AppStateDto(
        kafka_use_cases=[load_default_kafka_use_case("Kafka use case 1")],
        clickhouse_use_cases=[load_default_clickhouse_use_case("ClickHouse use case 1")],
        selected_kafka_index=0,
        selected_clickhouse_index=0,
    )


def convert_legacy_payload(payload: dict[str, object]) -> dict[str, object]:
    normalized = dict(payload)
    if "kafka_chats" in normalized and "kafka_use_cases" not in normalized:
        normalized["kafka_use_cases"] = normalized["kafka_chats"]
    if "clickhouse_chats" in normalized and "clickhouse_use_cases" not in normalized:
        normalized["clickhouse_use_cases"] = normalized["clickhouse_chats"]
    if isinstance(normalized.get("kafka_use_cases"), list):
        kafka_rows: list[dict[str, object]] = []
        for item in normalized["kafka_use_cases"]:
            if not isinstance(item, dict):
                continue
            row = dict(item)
            row.setdefault("name", "Kafka use case")
            row.setdefault("bootstrap_url", DEFAULT_KAFKA_BOOTSTRAP_URL)
            row.setdefault("topic", DEFAULT_KAFKA_TOPIC)
            row.setdefault("hooks", EXAMPLE_KAFKA_HOOKS)
            row.setdefault("messages", EXAMPLE_KAFKA_MESSAGES)
            if "global_interval_ms" in row and "global_timeout_ms" not in row:
                row["global_timeout_ms"] = row["global_interval_ms"]
            row.setdefault("global_timeout_ms", DEFAULT_GLOBAL_TIMEOUT_MS)
            row.pop("global_interval_ms", None)
            kafka_rows.append(row)
        normalized["kafka_use_cases"] = kafka_rows
    if isinstance(normalized.get("clickhouse_use_cases"), list):
        clickhouse_rows: list[dict[str, object]] = []
        for item in normalized["clickhouse_use_cases"]:
            if not isinstance(item, dict):
                continue
            row = dict(item)
            row.setdefault("name", "ClickHouse use case")
            if "endpoint" not in row:
                host = str(row.get("host", "localhost"))
                port = int(row.get("port", 8123))
                row["endpoint"] = f"{host}:{port}"
            row.setdefault("hooks", EXAMPLE_CLICKHOUSE_HOOKS)
            row.setdefault("messages", EXAMPLE_CLICKHOUSE_MESSAGES)
            if "global_interval_ms" in row and "global_timeout_ms" not in row:
                row["global_timeout_ms"] = row["global_interval_ms"]
            row.setdefault("global_timeout_ms", DEFAULT_GLOBAL_TIMEOUT_MS)
            row.pop("host", None)
            row.pop("port", None)
            row.pop("table_name", None)
            row.pop("raw_sql_mode", None)
            row.pop("global_interval_ms", None)
            clickhouse_rows.append(row)
        normalized["clickhouse_use_cases"] = clickhouse_rows
    normalized.setdefault("selected_kafka_index", 0)
    normalized.setdefault("selected_clickhouse_index", 0)
    return normalized


def dump_state_to_json(state: AppStateDto) -> str:
    return json.dumps(asdict(state), ensure_ascii=True, indent=2)


def load_state_from_payload(payload: dict[str, object]) -> AppStateDto:
    return RETORT_STATE.load(payload, AppStateDto)


def load_state_with_retort_scenarios(payload: dict[str, object]) -> AppStateDto:
    scenarios: list[dict[str, object]] = [
        payload,
        convert_legacy_payload(payload),
    ]
    for scenario_payload in scenarios:
        try:
            return load_state_from_payload(scenario_payload)
        except Exception:
            continue
    raise ValueError("state payload does not match supported schemas")


def convert_loaded_state(state: AppStateDto) -> AppStateDto:
    if not state.kafka_use_cases:
        state.kafka_use_cases = [load_default_kafka_use_case("Kafka use case 1")]
    if not state.clickhouse_use_cases:
        state.clickhouse_use_cases = [load_default_clickhouse_use_case("ClickHouse use case 1")]
    state.selected_kafka_index = max(0, min(state.selected_kafka_index, len(state.kafka_use_cases) - 1))
    state.selected_clickhouse_index = max(0, min(state.selected_clickhouse_index, len(state.clickhouse_use_cases) - 1))
    return state


def load_state_from_json(raw_text: str) -> AppStateDto:
    try:
        payload = json.loads(raw_text)
    except Exception:
        return load_default_state()
    if not isinstance(payload, dict):
        return load_default_state()
    try:
        state = load_state_with_retort_scenarios(payload)
    except Exception:
        return load_default_state()
    return convert_loaded_state(state)


def load_state(config_path: Path) -> AppStateDto:
    if not config_path.exists():
        return load_default_state()
    return load_state_from_json(config_path.read_text(encoding="utf-8"))


def dump_state(config_path: Path, state: AppStateDto) -> None:
    config_path.write_text(dump_state_to_json(state), encoding="utf-8")
