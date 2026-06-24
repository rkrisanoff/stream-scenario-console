import clickhouse_connect

import gradio as gr

from datetime import datetime
from pathlib import Path

from console.converters.state_converter import load_state
from console.dtos import AppStateDto
from console.parser import MessageParser
from console.persistence import (
    BACKUP_INTERVAL_SEC,
    CONFIG_PATH,
    SaveMeta,
    format_backup_status,
    format_status,
    immediate_save,
    initial_save_meta,
    mark_dirty,
    run_backup,
    save_if_dirty,
    should_run_scheduled_backup,
)
from console.runner import ScenarioRunner
from console.use_case_state import (
    LogsByUseCase,
    SidebarValue,
    add_clickhouse_use_case,
    add_kafka_use_case,
    apply_clickhouse_sidebar_value,
    apply_kafka_sidebar_value,
    build_clickhouse_sidebar_value,
    build_kafka_sidebar_value,
    clamp_index,
    persist_clickhouse_form,
    persist_kafka_form,
    update_clickhouse_use_case_form,
    update_kafka_use_case_form,
)


PARSER = MessageParser()


def _sidebar_ids(sidebar_value: SidebarValue) -> list[str]:
    raw_items = sidebar_value.get("items", [])
    if not isinstance(raw_items, list):
        return []
    ids: list[str] = []
    for item in raw_items:
        if isinstance(item, dict) and item.get("id") is not None:
            ids.append(str(item["id"]))
    return ids


def guard_sidebar_deletion(
    state: AppStateDto,
    sidebar_value: SidebarValue,
    allow_delete: bool,
    *,
    cases_attr: str,
    build_sidebar_value,
) -> SidebarValue:
    if allow_delete:
        return sidebar_value
    use_cases = getattr(state, cases_attr)
    if len(_sidebar_ids(sidebar_value)) >= len(use_cases):
        return sidebar_value
    return build_sidebar_value(state)


def purge_removed_logs(logs_by_use_case: LogsByUseCase, old_ids: set[str], new_ids: set[str]) -> LogsByUseCase:
    updated = dict(logs_by_use_case)
    for removed_id in old_ids - new_ids:
        updated.pop(removed_id, None)
    return updated


def store_logs(logs_by_use_case: LogsByUseCase, use_case_id: str, logs_text: str) -> LogsByUseCase:
    updated = dict(logs_by_use_case)
    updated[use_case_id] = logs_text
    return updated


def kafka_panel_updates(state: AppStateDto, logs_by_use_case: LogsByUseCase) -> tuple:
    selected_index = clamp_index(state.selected_kafka_index, len(state.kafka_use_cases))
    use_case = state.kafka_use_cases[selected_index]
    return (
        gr.update(value=build_kafka_sidebar_value(state)),
        gr.update(value=use_case.bootstrap_url),
        gr.update(value=use_case.topic),
        gr.update(value=use_case.hooks),
        gr.update(value=use_case.messages),
        gr.update(value=use_case.global_timeout_ms),
        gr.update(value=logs_by_use_case.get(use_case.id, "")),
    )


def clickhouse_panel_updates(state: AppStateDto, logs_by_use_case: LogsByUseCase) -> tuple:
    selected_index = clamp_index(state.selected_clickhouse_index, len(state.clickhouse_use_cases))
    use_case = state.clickhouse_use_cases[selected_index]
    return (
        gr.update(value=build_clickhouse_sidebar_value(state)),
        gr.update(value=use_case.host),
        gr.update(value=use_case.port),
        gr.update(value=use_case.user),
        gr.update(value=use_case.password),
        gr.update(value=use_case.database),
        gr.update(value=use_case.hooks),
        gr.update(value=use_case.messages),
        gr.update(value=use_case.global_timeout_ms),
        gr.update(value=logs_by_use_case.get(use_case.id, "")),
    )


async def send_kafka_use_case(
    state: AppStateDto,
    logs_by_use_case: LogsByUseCase,
    use_case_id: str,
    bootstrap_url: str,
    topic: str,
    hooks: str,
    messages: str,
    global_timeout_ms: float,
) -> tuple[AppStateDto, LogsByUseCase, str, str]:
    state = update_kafka_use_case_form(state, use_case_id, bootstrap_url, topic, hooks, messages, global_timeout_ms)
    logs: list[str] = []
    runner = ScenarioRunner(log_handler=logs.append)
    try:
        from aiokafka import AIOKafkaProducer
    except Exception as exc:
        logs_text = f"error: {exc}"
        return state, store_logs(logs_by_use_case, use_case_id, logs_text), f"error: {exc}", logs_text
    try:
        parsed = PARSER.parse_messages(messages, int(global_timeout_ms), hooks)
        commands = runner.build_kafka_commands(parsed, topic)
        producer = AIOKafkaProducer(bootstrap_servers=bootstrap_url)
        runner.emit_log(f"kafka producer start -> {bootstrap_url}")
        await producer.start()
        try:
            await runner.run_kafka_commands(producer, commands)
        finally:
            await producer.stop()
            runner.emit_log("kafka producer stop")
    except Exception as exc:
        runner.emit_log(f"error: {exc}")
        logs_text = "\n".join(logs)
        return state, store_logs(logs_by_use_case, use_case_id, logs_text), f"error: {exc}", logs_text
    logs_text = "\n".join(logs)
    return state, store_logs(logs_by_use_case, use_case_id, logs_text), f"ok: sent {len(parsed)}", logs_text


def send_clickhouse_use_case(
    state: AppStateDto,
    logs_by_use_case: LogsByUseCase,
    use_case_id: str,
    host: str,
    port: float,
    user: str,
    password: str,
    database: str,
    hooks: str,
    messages: str,
    global_timeout_ms: float,
) -> tuple[AppStateDto, LogsByUseCase, str, str]:
    state = update_clickhouse_use_case_form(
        state, use_case_id, host, port, user, password, database, hooks, messages, global_timeout_ms
    )
    logs: list[str] = []
    runner = ScenarioRunner(log_handler=logs.append)
    try:
        actions = PARSER.parse_clickhouse_messages(messages, int(global_timeout_ms), hooks)
        commands = runner.build_clickhouse_commands(actions)
        client = clickhouse_connect.get_client(
            host=host.strip(),
            port=int(port),
            username=user.strip(),
            password=password,
            database=database.strip(),
        )
        try:
            runner.emit_log(f"clickhouse connect -> {host}:{int(port)}/{database}")
            runner.run_clickhouse_commands(client, commands)
        finally:
            client.close()
            runner.emit_log("clickhouse client close")
    except Exception as exc:
        runner.emit_log(f"error: {exc}")
        logs_text = "\n".join(logs)
        return state, store_logs(logs_by_use_case, use_case_id, logs_text), f"error: {exc}", logs_text
    logs_text = "\n".join(logs)
    return state, store_logs(logs_by_use_case, use_case_id, logs_text), f"ok: sent {len(actions)}", logs_text


def save_all(state: AppStateDto, save_meta: SaveMeta) -> tuple[AppStateDto, SaveMeta, str]:
    new_meta, status = immediate_save(CONFIG_PATH, state, save_meta, message="manual")
    return state, new_meta, status


def reload_from_disk() -> tuple:
    state = load_state(CONFIG_PATH)
    kafka_updates = kafka_panel_updates(state, {})
    clickhouse_updates = clickhouse_panel_updates(state, {})
    new_meta = initial_save_meta(CONFIG_PATH.exists())
    return (state, new_meta, format_status(new_meta), {}, {}, "", "", *kafka_updates, *clickhouse_updates)


def load_all(_state: AppStateDto, _save_meta: SaveMeta) -> tuple:
    return reload_from_disk()


def on_autosave_tick(state: AppStateDto, save_meta: SaveMeta) -> tuple[SaveMeta, str]:
    new_meta = save_if_dirty(CONFIG_PATH, state, save_meta)
    return new_meta, format_status(new_meta)


def on_backup_tick(state: AppStateDto, last_backup_at: datetime | None) -> datetime | None:
    if not should_run_scheduled_backup(last_backup_at, BACKUP_INTERVAL_SEC):
        return last_backup_at
    run_backup(state)
    return datetime.now()


def on_manual_backup(state: AppStateDto, last_backup_at: datetime | None) -> tuple[datetime, str]:
    backup_path = run_backup(state)
    backed_up_at = datetime.now()
    return backed_up_at, format_backup_status(backup_path)


def on_kafka_sidebar_change(
    sidebar_value: SidebarValue,
    state: AppStateDto,
    logs_by_use_case: LogsByUseCase,
    save_meta: SaveMeta,
    allow_delete: bool,
    bootstrap_url: str,
    topic: str,
    hooks: str,
    messages: str,
    global_timeout_ms: float,
) -> tuple:
    state = persist_kafka_form(state, bootstrap_url, topic, hooks, messages, global_timeout_ms)
    sidebar_value = guard_sidebar_deletion(
        state,
        sidebar_value,
        allow_delete,
        cases_attr="kafka_use_cases",
        build_sidebar_value=build_kafka_sidebar_value,
    )
    old_ids = {item.id for item in state.kafka_use_cases}
    state = apply_kafka_sidebar_value(state, sidebar_value)
    new_ids = {item.id for item in state.kafka_use_cases}
    updated_logs = purge_removed_logs(logs_by_use_case, old_ids, new_ids)
    save_meta, save_status_text = immediate_save(CONFIG_PATH, state, save_meta)
    return (state, updated_logs, save_meta, save_status_text, *kafka_panel_updates(state, updated_logs))


def on_clickhouse_sidebar_change(
    sidebar_value: SidebarValue,
    state: AppStateDto,
    logs_by_use_case: LogsByUseCase,
    save_meta: SaveMeta,
    allow_delete: bool,
    host: str,
    port: float,
    user: str,
    password: str,
    database: str,
    hooks: str,
    messages: str,
    global_timeout_ms: float,
) -> tuple:
    state = persist_clickhouse_form(state, host, port, user, password, database, hooks, messages, global_timeout_ms)
    sidebar_value = guard_sidebar_deletion(
        state,
        sidebar_value,
        allow_delete,
        cases_attr="clickhouse_use_cases",
        build_sidebar_value=build_clickhouse_sidebar_value,
    )
    old_ids = {item.id for item in state.clickhouse_use_cases}
    state = apply_clickhouse_sidebar_value(state, sidebar_value)
    new_ids = {item.id for item in state.clickhouse_use_cases}
    updated_logs = purge_removed_logs(logs_by_use_case, old_ids, new_ids)
    save_meta, save_status_text = immediate_save(CONFIG_PATH, state, save_meta)
    return (state, updated_logs, save_meta, save_status_text, *clickhouse_panel_updates(state, updated_logs))


def on_kafka_sidebar_add(
    state: AppStateDto,
    logs_by_use_case: LogsByUseCase,
    save_meta: SaveMeta,
    bootstrap_url: str,
    topic: str,
    hooks: str,
    messages: str,
    global_timeout_ms: float,
) -> tuple:
    state = persist_kafka_form(state, bootstrap_url, topic, hooks, messages, global_timeout_ms)
    state, status = add_kafka_use_case(state)
    save_meta, save_status_text = immediate_save(CONFIG_PATH, state, save_meta)
    return (state, *kafka_panel_updates(state, logs_by_use_case), status, save_meta, save_status_text)


def on_clickhouse_sidebar_add(
    state: AppStateDto,
    logs_by_use_case: LogsByUseCase,
    save_meta: SaveMeta,
    host: str,
    port: float,
    user: str,
    password: str,
    database: str,
    hooks: str,
    messages: str,
    global_timeout_ms: float,
) -> tuple:
    state = persist_clickhouse_form(state, host, port, user, password, database, hooks, messages, global_timeout_ms)
    state, status = add_clickhouse_use_case(state)
    save_meta, save_status_text = immediate_save(CONFIG_PATH, state, save_meta)
    return (state, *clickhouse_panel_updates(state, logs_by_use_case), status, save_meta, save_status_text)


async def on_kafka_send(
    state: AppStateDto,
    logs_by_use_case: LogsByUseCase,
    save_meta: SaveMeta,
    bootstrap_url: str,
    topic: str,
    hooks: str,
    messages: str,
    global_timeout_ms: float,
) -> tuple:
    state = persist_kafka_form(state, bootstrap_url, topic, hooks, messages, global_timeout_ms)
    selected_index = clamp_index(state.selected_kafka_index, len(state.kafka_use_cases))
    use_case_id = state.kafka_use_cases[selected_index].id
    state, logs_by_use_case, status, _ = await send_kafka_use_case(
        state,
        logs_by_use_case,
        use_case_id,
        bootstrap_url,
        topic,
        hooks,
        messages,
        global_timeout_ms,
    )
    save_meta, save_status_text = immediate_save(CONFIG_PATH, state, save_meta)
    return (state, logs_by_use_case, *kafka_panel_updates(state, logs_by_use_case), status, save_meta, save_status_text)


def on_clickhouse_send(
    state: AppStateDto,
    logs_by_use_case: LogsByUseCase,
    save_meta: SaveMeta,
    host: str,
    port: float,
    user: str,
    password: str,
    database: str,
    hooks: str,
    messages: str,
    global_timeout_ms: float,
) -> tuple:
    state = persist_clickhouse_form(state, host, port, user, password, database, hooks, messages, global_timeout_ms)
    selected_index = clamp_index(state.selected_clickhouse_index, len(state.clickhouse_use_cases))
    use_case_id = state.clickhouse_use_cases[selected_index].id
    state, logs_by_use_case, status, _ = send_clickhouse_use_case(
        state,
        logs_by_use_case,
        use_case_id,
        host,
        port,
        user,
        password,
        database,
        hooks,
        messages,
        global_timeout_ms,
    )
    save_meta, save_status_text = immediate_save(CONFIG_PATH, state, save_meta)
    return (state, logs_by_use_case, *clickhouse_panel_updates(state, logs_by_use_case), status, save_meta, save_status_text)


def on_kafka_field_change(
    state: AppStateDto,
    save_meta: SaveMeta,
    bootstrap_url: str,
    topic: str,
    hooks: str,
    messages: str,
    global_timeout_ms: float,
) -> tuple[AppStateDto, SaveMeta, str]:
    state = persist_kafka_form(state, bootstrap_url, topic, hooks, messages, global_timeout_ms)
    new_meta = mark_dirty(save_meta)
    return state, new_meta, format_status(new_meta)


def on_clickhouse_field_change(
    state: AppStateDto,
    save_meta: SaveMeta,
    host: str,
    port: float,
    user: str,
    password: str,
    database: str,
    hooks: str,
    messages: str,
    global_timeout_ms: float,
) -> tuple[AppStateDto, SaveMeta, str]:
    state = persist_clickhouse_form(state, host, port, user, password, database, hooks, messages, global_timeout_ms)
    new_meta = mark_dirty(save_meta)
    return state, new_meta, format_status(new_meta)
