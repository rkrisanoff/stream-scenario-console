import asyncio
from dataclasses import dataclass
import json
from time import sleep
from uuid import uuid4

import clickhouse_connect
from aiokafka import AIOKafkaProducer


@dataclass(slots=True)
class KafkaConfig:
    bootstrap_url: str = "localhost:9092"
    topic: str = "events"


@dataclass(slots=True)
class ClickHouseConfig:
    host: str = "localhost"
    port: int = 8123
    user: str = "default"
    password: str = ""
    database: str = "default"
    table_name: str = "events"


def build_kafka_config() -> KafkaConfig:
    return KafkaConfig()


def build_clickhouse_config() -> ClickHouseConfig:
    return ClickHouseConfig()


async def create_kafka_producer(config: KafkaConfig) -> AIOKafkaProducer:
    producer = AIOKafkaProducer(bootstrap_servers=config.bootstrap_url)
    await producer.start()
    return producer


def create_clickhouse_client(config: ClickHouseConfig):
    return clickhouse_connect.get_client(
        host=config.host,
        port=config.port,
        username=config.user,
        password=config.password,
        database=config.database,
    )


async def do_kafka_use_case_1() -> None:
    config = build_kafka_config()
    producer = await create_kafka_producer(config)
    try:
        event_id = str(uuid4())
        opened_payload = {"event": "open", "event_id": event_id}
        clicked_payload = {"event": "click", "event_id": event_id}
        await producer.send_and_wait(config.topic, json.dumps(opened_payload).encode("utf-8"))
        await asyncio.sleep(2.0)
        await producer.send_and_wait(config.topic, json.dumps(clicked_payload).encode("utf-8"))
    finally:
        await producer.stop()


def do_clickhouse_use_case_1() -> None:
    config = build_clickhouse_config()
    client = create_clickhouse_client(config)
    try:
        event_id = str(uuid4())
        opened_row = {"event": "open", "event_id": event_id}
        clicked_row = {"event": "click", "event_id": event_id}
        client.insert(config.table_name, [list(opened_row.values())], column_names=list(opened_row.keys()))
        sleep(2.0)
        client.insert(config.table_name, [list(clicked_row.values())], column_names=list(clicked_row.keys()))
    finally:
        client.close()


def do_clickhouse_use_case_2() -> None:
    config = build_clickhouse_config()
    client = create_clickhouse_client(config)
    try:
        event_id = str(uuid4())
        client.command(f"OPTIMIZE TABLE {config.table_name} FINAL")
        sleep(1.5)
        row = {"event": "optimize_done", "event_id": event_id}
        client.insert(config.table_name, [list(row.values())], column_names=list(row.keys()))
    finally:
        client.close()
