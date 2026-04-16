from dataclasses import dataclass


@dataclass(slots=True)
class KafkaUseCaseDto:
    name: str
    bootstrap_url: str
    topic: str
    hooks: str
    messages: str
    global_timeout_ms: int


@dataclass(slots=True)
class ClickHouseUseCaseDto:
    name: str
    endpoint: str
    hooks: str
    messages: str
    global_timeout_ms: int


@dataclass(slots=True)
class AppStateDto:
    kafka_use_cases: list[KafkaUseCaseDto]
    clickhouse_use_cases: list[ClickHouseUseCaseDto]
    selected_kafka_index: int
    selected_clickhouse_index: int


@dataclass(slots=True)
class KafkaViewDto:
    state: AppStateDto
    rows_update: object
    name: str
    bootstrap_url: str
    topic: str
    hooks: str
    messages: str
    global_timeout_ms: int
    status: str


@dataclass(slots=True)
class ClickHouseViewDto:
    state: AppStateDto
    rows_update: object
    name: str
    endpoint: str
    hooks: str
    messages: str
    global_timeout_ms: int
    status: str


@dataclass(slots=True)
class LoadViewDto:
    state: AppStateDto
    kafka: KafkaViewDto
    clickhouse: ClickHouseViewDto
    save_status: str
