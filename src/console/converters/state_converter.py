from dataclasses import asdict
import json
from pathlib import Path

from adaptix import Retort

from console.dtos import AppStateDto, ClickHouseUseCaseDto, KafkaUseCaseDto
from console.help_texts import (
    DEFAULT_CLICKHOUSE_DATABASE,
    DEFAULT_CLICKHOUSE_HOST,
    DEFAULT_CLICKHOUSE_PASSWORD,
    DEFAULT_CLICKHOUSE_PORT,
    DEFAULT_CLICKHOUSE_USER,
    DEFAULT_GLOBAL_TIMEOUT_MS,
    DEFAULT_KAFKA_BOOTSTRAP_URL,
    DEFAULT_KAFKA_TOPIC,
    EXAMPLE_CLICKHOUSE_HOOKS,
    EXAMPLE_CLICKHOUSE_MESSAGES,
    EXAMPLE_KAFKA_HOOKS,
    EXAMPLE_KAFKA_MESSAGES,
)


RETORT_STATE = Retort()

type JsonPrimitive = str | int | float | bool | None
type JsonValue = JsonPrimitive | list["JsonValue"] | dict[str, "JsonValue"]
type JsonDict = dict[str, JsonValue]


def convert_int(value: JsonValue, fallback: int) -> int:
    try:
        return int(value)
    except Exception:
        return fallback


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
        host=DEFAULT_CLICKHOUSE_HOST,
        port=DEFAULT_CLICKHOUSE_PORT,
        user=DEFAULT_CLICKHOUSE_USER,
        password=DEFAULT_CLICKHOUSE_PASSWORD,
        database=DEFAULT_CLICKHOUSE_DATABASE,
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


def convert_payload(payload: JsonDict) -> JsonDict:
    normalized = dict(payload)
    if isinstance(normalized.get("kafka_use_cases"), list):
        kafka_rows: list[JsonDict] = []
        for item in normalized["kafka_use_cases"]:
            if not isinstance(item, dict):
                continue
            row = {
                "name": item.get("name", "Kafka use case"),
                "bootstrap_url": item.get("bootstrap_url", DEFAULT_KAFKA_BOOTSTRAP_URL),
                "topic": item.get("topic", DEFAULT_KAFKA_TOPIC),
                "hooks": item.get("hooks", EXAMPLE_KAFKA_HOOKS),
                "messages": item.get("messages", EXAMPLE_KAFKA_MESSAGES),
                "global_timeout_ms": convert_int(item.get("global_timeout_ms"), DEFAULT_GLOBAL_TIMEOUT_MS),
            }
            kafka_rows.append(row)
        normalized["kafka_use_cases"] = kafka_rows
    if isinstance(normalized.get("clickhouse_use_cases"), list):
        clickhouse_rows: list[JsonDict] = []
        for item in normalized["clickhouse_use_cases"]:
            if not isinstance(item, dict):
                continue
            row = {
                "name": item.get("name", "ClickHouse use case"),
                "host": item.get("host", DEFAULT_CLICKHOUSE_HOST),
                "port": convert_int(item.get("port"), DEFAULT_CLICKHOUSE_PORT),
                "user": item.get("user", DEFAULT_CLICKHOUSE_USER),
                "password": item.get("password", DEFAULT_CLICKHOUSE_PASSWORD),
                "database": item.get("database", DEFAULT_CLICKHOUSE_DATABASE),
                "hooks": item.get("hooks", EXAMPLE_CLICKHOUSE_HOOKS),
                "messages": item.get("messages", EXAMPLE_CLICKHOUSE_MESSAGES),
                "global_timeout_ms": convert_int(item.get("global_timeout_ms"), DEFAULT_GLOBAL_TIMEOUT_MS),
            }
            clickhouse_rows.append(row)
        normalized["clickhouse_use_cases"] = clickhouse_rows
    normalized["selected_kafka_index"] = convert_int(normalized.get("selected_kafka_index"), 0)
    normalized["selected_clickhouse_index"] = convert_int(normalized.get("selected_clickhouse_index"), 0)
    return normalized


def dump_state_to_json(state: AppStateDto) -> str:
    return json.dumps(asdict(state), ensure_ascii=True, indent=2)


def load_state_from_payload(payload: JsonDict) -> AppStateDto:
    return RETORT_STATE.load(payload, AppStateDto)


def load_state_with_retort_scenarios(payload: JsonDict) -> AppStateDto:
    return load_state_from_payload(convert_payload(payload))


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
