from dataclasses import dataclass

type GradioRowsUpdate = dict[str, str | list[list[str]]]


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


@dataclass(slots=True)
class KafkaViewDto:
    state: AppStateDto
    rows_update: GradioRowsUpdate
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
    rows_update: GradioRowsUpdate
    name: str
    host: str
    port: int
    user: str
    password: str
    database: str
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
