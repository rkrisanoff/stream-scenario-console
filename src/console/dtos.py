from dataclasses import dataclass


@dataclass(slots=True)
class KafkaUseCaseDto:
    id: str
    name: str
    bootstrap_url: str
    topic: str
    hooks: str
    messages: str
    global_timeout_ms: int


@dataclass(slots=True)
class ClickHouseUseCaseDto:
    id: str
    name: str
    host: str
    port: int
    user: str
    password: str
    database: str
    hooks: str
    messages: str
    global_timeout_ms: int


@dataclass(slots=True)
class AppStateDto:
    kafka_use_cases: list[KafkaUseCaseDto]
    clickhouse_use_cases: list[ClickHouseUseCaseDto]
    selected_kafka_index: int
    selected_clickhouse_index: int
