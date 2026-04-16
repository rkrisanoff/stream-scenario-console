import asyncio
import json
from pathlib import Path
import time

import gradio as gr

from console.converters.state_converter import (
    dump_state,
    load_default_clickhouse_use_case,
    load_default_kafka_use_case,
    load_default_state,
    load_state,
)
from console.dtos import (
    AppStateDto as AppState,
    ClickHouseViewDto as ClickHouseView,
    KafkaViewDto as KafkaView,
    LoadViewDto as LoadView,
)
from console.help_texts import build_main_help_columns
from console.parser import DIRECTIVE_CLICKHOUSE_TABLE, DIRECTIVE_TIMEOUT, MessageParser


CONFIG_PATH = Path("configuration.json")
STATIC_DIR = Path(__file__).resolve().parent / "static"
APP_CSS = (STATIC_DIR / "app.css").read_text(encoding="utf-8")
APP_JS = (STATIC_DIR / "app.js").read_text(encoding="utf-8")
PARSER = MessageParser()


def default_state() -> AppState:
    return load_default_state()


def kafka_rows(state: AppState) -> list[list[str]]:
    return [[item.name] for item in state.kafka_use_cases]


def clickhouse_rows(state: AppState) -> list[list[str]]:
    return [[item.name] for item in state.clickhouse_use_cases]


def kafka_view(state: AppState, status: str = "") -> KafkaView:
    use_case = state.kafka_use_cases[state.selected_kafka_index]
    return KafkaView(
        state=state,
        rows_update=gr.update(value=kafka_rows(state)),
        name=use_case.name,
        bootstrap_url=use_case.bootstrap_url,
        topic=use_case.topic,
        hooks=use_case.hooks,
        messages=use_case.messages,
        global_timeout_ms=use_case.global_timeout_ms,
        status=status,
    )


def clickhouse_view(state: AppState, status: str = "") -> ClickHouseView:
    use_case = state.clickhouse_use_cases[state.selected_clickhouse_index]
    return ClickHouseView(
        state=state,
        rows_update=gr.update(value=clickhouse_rows(state)),
        name=use_case.name,
        endpoint=use_case.endpoint,
        hooks=use_case.hooks,
        messages=use_case.messages,
        global_timeout_ms=use_case.global_timeout_ms,
        status=status,
    )


def render_kafka_view(view: KafkaView) -> list[object]:
    return [
        view.state,
        view.rows_update,
        view.name,
        view.bootstrap_url,
        view.topic,
        view.hooks,
        view.messages,
        view.global_timeout_ms,
        view.status,
    ]


def render_clickhouse_view(view: ClickHouseView) -> list[object]:
    return [
        view.state,
        view.rows_update,
        view.name,
        view.endpoint,
        view.hooks,
        view.messages,
        view.global_timeout_ms,
        view.status,
    ]


def render_load_view(view: LoadView) -> list[object]:
    return [
        view.state,
        view.kafka.rows_update,
        view.kafka.name,
        view.kafka.bootstrap_url,
        view.kafka.topic,
        view.kafka.hooks,
        view.kafka.messages,
        view.kafka.global_timeout_ms,
        view.kafka.status,
        view.clickhouse.rows_update,
        view.clickhouse.name,
        view.clickhouse.endpoint,
        view.clickhouse.hooks,
        view.clickhouse.messages,
        view.clickhouse.global_timeout_ms,
        view.clickhouse.status,
        view.save_status,
    ]


def load_rendered_view() -> list[object]:
    return render_load_view(load_all())


def select_kafka_rendered(state: AppState, evt: gr.SelectData) -> list[object]:
    return render_kafka_view(select_kafka(state, evt))


def select_clickhouse_rendered(state: AppState, evt: gr.SelectData) -> list[object]:
    return render_clickhouse_view(select_clickhouse(state, evt))


def extract_row_index(raw_index: int | tuple[int, int] | list[object]) -> int:
    if isinstance(raw_index, tuple):
        return int(raw_index[0])
    if isinstance(raw_index, list):
        return int(raw_index[0]) if raw_index else -1
    return int(raw_index)


def parse_endpoint(endpoint: str) -> tuple[str, int]:
    host, _, port = endpoint.strip().rpartition(":")
    if not host or not port:
        raise ValueError("endpoint must be host:port")
    return host, int(port)


def save_kafka_form(state: AppState, bootstrap_url: str, topic: str, hooks: str, messages: str, global_timeout_ms: float) -> AppState:
    use_case = state.kafka_use_cases[state.selected_kafka_index]
    use_case.bootstrap_url = bootstrap_url.strip()
    use_case.topic = topic
    use_case.hooks = hooks
    use_case.messages = messages
    use_case.global_timeout_ms = int(global_timeout_ms)
    return state


def save_clickhouse_form(state: AppState, endpoint: str, hooks: str, messages: str, global_timeout_ms: float) -> AppState:
    use_case = state.clickhouse_use_cases[state.selected_clickhouse_index]
    use_case.endpoint = endpoint.strip()
    use_case.hooks = hooks
    use_case.messages = messages
    use_case.global_timeout_ms = int(global_timeout_ms)
    return state


def select_kafka(state: AppState, evt: gr.SelectData) -> KafkaView:
    index = extract_row_index(evt.index)
    if 0 <= index < len(state.kafka_use_cases):
        state.selected_kafka_index = index
    return kafka_view(state)


def select_clickhouse(state: AppState, evt: gr.SelectData) -> ClickHouseView:
    index = extract_row_index(evt.index)
    if 0 <= index < len(state.clickhouse_use_cases):
        state.selected_clickhouse_index = index
    return clickhouse_view(state)


def add_kafka(state: AppState) -> KafkaView:
    use_case_name = f"Kafka use case {len(state.kafka_use_cases) + 1}"
    state.kafka_use_cases.append(load_default_kafka_use_case(use_case_name))
    state.selected_kafka_index = len(state.kafka_use_cases) - 1
    return kafka_view(state, "ok: use case added")


def add_clickhouse(state: AppState) -> ClickHouseView:
    use_case_name = f"ClickHouse use case {len(state.clickhouse_use_cases) + 1}"
    state.clickhouse_use_cases.append(load_default_clickhouse_use_case(use_case_name))
    state.selected_clickhouse_index = len(state.clickhouse_use_cases) - 1
    return clickhouse_view(state, "ok: use case added")


def rename_kafka(state: AppState, name: str) -> KafkaView:
    if not name.strip():
        return kafka_view(state, "error: empty use case name")
    state.kafka_use_cases[state.selected_kafka_index].name = name.strip()
    return kafka_view(state, "ok: renamed")


def rename_clickhouse(state: AppState, name: str) -> ClickHouseView:
    if not name.strip():
        return clickhouse_view(state, "error: empty use case name")
    state.clickhouse_use_cases[state.selected_clickhouse_index].name = name.strip()
    return clickhouse_view(state, "ok: renamed")


def delete_kafka(state: AppState) -> KafkaView:
    if len(state.kafka_use_cases) == 1:
        return kafka_view(state, "error: cannot delete last use case")
    state.kafka_use_cases.pop(state.selected_kafka_index)
    state.selected_kafka_index = max(0, min(state.selected_kafka_index, len(state.kafka_use_cases) - 1))
    return kafka_view(state, "ok: deleted")


def delete_clickhouse(state: AppState) -> ClickHouseView:
    if len(state.clickhouse_use_cases) == 1:
        return clickhouse_view(state, "error: cannot delete last use case")
    state.clickhouse_use_cases.pop(state.selected_clickhouse_index)
    state.selected_clickhouse_index = max(0, min(state.selected_clickhouse_index, len(state.clickhouse_use_cases) - 1))
    return clickhouse_view(state, "ok: deleted")


async def send_kafka(state: AppState, bootstrap_url: str, topic: str, hooks: str, messages: str, global_timeout_ms: float) -> tuple[AppState, str]:
    save_kafka_form(state, bootstrap_url, topic, hooks, messages, global_timeout_ms)
    try:
        from aiokafka import AIOKafkaProducer
    except Exception as exc:
        return state, f"error: {exc}"
    try:
        parsed = PARSER.parse_messages(messages, int(global_timeout_ms), hooks)
        producer = AIOKafkaProducer(bootstrap_servers=bootstrap_url)
        await producer.start()
        try:
            for index, item in enumerate(parsed):
                await producer.send_and_wait(topic, item.text.encode("utf-8"))
                if index < len(parsed) - 1:
                    await asyncio.sleep(item.delay_ms / 1000)
        finally:
            await producer.stop()
    except Exception as exc:
        return state, f"error: {exc}"
    return state, f"ok: sent {len(parsed)}"


def send_clickhouse(state: AppState, endpoint: str, hooks: str, messages: str, global_timeout_ms: float) -> tuple[AppState, str]:
    save_clickhouse_form(state, endpoint, hooks, messages, global_timeout_ms)
    try:
        import clickhouse_connect
    except Exception as exc:
        return state, f"error: {exc}"
    try:
        host, port = parse_endpoint(endpoint)
        actions = PARSER.parse_clickhouse_messages(messages, int(global_timeout_ms), hooks)
        client = clickhouse_connect.get_client(host=host, port=port)
        for index, action in enumerate(actions):
            if action.kind == "json":
                row = json.loads(action.text)
                if not isinstance(row, dict):
                    raise ValueError("json message must be object")
                client.insert(action.table_name, [list(row.values())], column_names=list(row.keys()))
            else:
                client.command(action.text)
            if index < len(actions) - 1:
                time.sleep(action.delay_ms / 1000)
    except Exception as exc:
        return state, f"error: {exc}"
    return state, f"ok: sent {len(actions)}"


def save_all(
    state: AppState,
    kafka_url: str,
    kafka_topic: str,
    kafka_hooks: str,
    kafka_messages: str,
    kafka_timeout: float,
    clickhouse_endpoint: str,
    clickhouse_hooks: str,
    clickhouse_messages: str,
    clickhouse_timeout: float,
) -> tuple[AppState, str]:
    save_kafka_form(state, kafka_url, kafka_topic, kafka_hooks, kafka_messages, kafka_timeout)
    save_clickhouse_form(state, clickhouse_endpoint, clickhouse_hooks, clickhouse_messages, clickhouse_timeout)
    dump_state(CONFIG_PATH, state)
    return state, "ok: configuration saved"


def load_all() -> LoadView:
    state = load_state(CONFIG_PATH)
    return LoadView(
        state=state,
        kafka=kafka_view(state),
        clickhouse=clickhouse_view(state),
        save_status="ok: configuration loaded",
    )


initial = load_state(CONFIG_PATH)
initial_kafka = initial.kafka_use_cases[initial.selected_kafka_index]
initial_clickhouse = initial.clickhouse_use_cases[initial.selected_clickhouse_index]

with gr.Blocks(title="QA ClickHouse Kafka Helper", fill_width=True) as app:
    state = gr.State(initial)
    gr.Markdown("## QA ClickHouse Kafka Helper")
    help_left, help_right = build_main_help_columns(DIRECTIVE_TIMEOUT, DIRECTIVE_CLICKHOUSE_TABLE)
    with gr.Row():
        with gr.Column(scale=1):
            gr.Markdown(help_left)
        with gr.Column(scale=1):
            gr.Markdown(help_right)
    with gr.Row():
        save_button = gr.Button("Save to File")
        load_button = gr.Button("Load from File")
    save_status = gr.Textbox(label="Save status", interactive=False, value="")

    with gr.Tabs():
        with gr.Tab("Kafka"):
            with gr.Row():
                with gr.Column(scale=0, min_width=280):
                    kafka_list = gr.Dataframe(headers=["Use case"], datatype=["str"], value=kafka_rows(initial), interactive=False, type="array", elem_id="kafka-use-cases")
                    kafka_name = gr.Textbox(label="Use case name", value=initial_kafka.name)
                    with gr.Row():
                        kafka_add = gr.Button("+")
                        kafka_rename = gr.Button("✎")
                        kafka_delete = gr.Button("🗑")
                with gr.Column(scale=4):
                    kafka_url = gr.Textbox(label="Bootstrap URL", value=initial_kafka.bootstrap_url)
                    kafka_topic = gr.Textbox(label="Topic", value=initial_kafka.topic)
                    kafka_hooks = gr.Code(label="Hooks", language="python", lines=10, value=initial_kafka.hooks, elem_id="kafka-hooks")
                    kafka_messages = gr.Code(label="Messages", language="json", lines=14, value=initial_kafka.messages, elem_id="kafka-messages")
                    kafka_timeout = gr.Number(label="Global timeout (ms)", precision=0, value=initial_kafka.global_timeout_ms)
                    kafka_send = gr.Button("Send")
                    kafka_status = gr.Textbox(label="Status", interactive=False, value="")

        with gr.Tab("ClickHouse"):
            with gr.Row():
                with gr.Column(scale=0, min_width=280):
                    clickhouse_list = gr.Dataframe(headers=["Use case"], datatype=["str"], value=clickhouse_rows(initial), interactive=False, type="array", elem_id="clickhouse-use-cases")
                    clickhouse_name = gr.Textbox(label="Use case name", value=initial_clickhouse.name)
                    with gr.Row():
                        clickhouse_add = gr.Button("+")
                        clickhouse_rename = gr.Button("✎")
                        clickhouse_delete = gr.Button("🗑")
                with gr.Column(scale=4):
                    clickhouse_endpoint = gr.Textbox(label="Endpoint", value=initial_clickhouse.endpoint)
                    clickhouse_hooks = gr.Code(label="Hooks", language="python", lines=10, value=initial_clickhouse.hooks, elem_id="clickhouse-hooks")
                    clickhouse_messages = gr.Code(label="Messages", language="sql", lines=14, value=initial_clickhouse.messages, elem_id="clickhouse-messages")
                    clickhouse_timeout = gr.Number(label="Global timeout (ms)", precision=0, value=initial_clickhouse.global_timeout_ms)
                    clickhouse_send = gr.Button("Send")
                    clickhouse_status = gr.Textbox(label="Status", interactive=False, value="")

    save_button.click(save_all, [state, kafka_url, kafka_topic, kafka_hooks, kafka_messages, kafka_timeout, clickhouse_endpoint, clickhouse_hooks, clickhouse_messages, clickhouse_timeout], [state, save_status])
    load_button.click(
        load_rendered_view,
        [],
        [state, kafka_list, kafka_name, kafka_url, kafka_topic, kafka_hooks, kafka_messages, kafka_timeout, kafka_status, clickhouse_list, clickhouse_name, clickhouse_endpoint, clickhouse_hooks, clickhouse_messages, clickhouse_timeout, clickhouse_status, save_status],
    )

    kafka_list.select(
        select_kafka_rendered,
        [state],
        [state, kafka_list, kafka_name, kafka_url, kafka_topic, kafka_hooks, kafka_messages, kafka_timeout, kafka_status],
    )
    kafka_add.click(
        lambda app_state: render_kafka_view(add_kafka(app_state)),
        [state],
        [state, kafka_list, kafka_name, kafka_url, kafka_topic, kafka_hooks, kafka_messages, kafka_timeout, kafka_status],
    )
    kafka_rename.click(
        lambda app_state, name: render_kafka_view(rename_kafka(app_state, name)),
        [state, kafka_name],
        [state, kafka_list, kafka_name, kafka_url, kafka_topic, kafka_hooks, kafka_messages, kafka_timeout, kafka_status],
    )
    kafka_delete.click(
        lambda app_state: render_kafka_view(delete_kafka(app_state)),
        [state],
        [state, kafka_list, kafka_name, kafka_url, kafka_topic, kafka_hooks, kafka_messages, kafka_timeout, kafka_status],
    )
    kafka_url.change(save_kafka_form, [state, kafka_url, kafka_topic, kafka_hooks, kafka_messages, kafka_timeout], [state])
    kafka_topic.change(save_kafka_form, [state, kafka_url, kafka_topic, kafka_hooks, kafka_messages, kafka_timeout], [state])
    kafka_hooks.change(save_kafka_form, [state, kafka_url, kafka_topic, kafka_hooks, kafka_messages, kafka_timeout], [state])
    kafka_messages.change(save_kafka_form, [state, kafka_url, kafka_topic, kafka_hooks, kafka_messages, kafka_timeout], [state])
    kafka_timeout.change(save_kafka_form, [state, kafka_url, kafka_topic, kafka_hooks, kafka_messages, kafka_timeout], [state])
    kafka_send.click(send_kafka, [state, kafka_url, kafka_topic, kafka_hooks, kafka_messages, kafka_timeout], [state, kafka_status])

    clickhouse_list.select(
        select_clickhouse_rendered,
        [state],
        [state, clickhouse_list, clickhouse_name, clickhouse_endpoint, clickhouse_hooks, clickhouse_messages, clickhouse_timeout, clickhouse_status],
    )
    clickhouse_add.click(
        lambda app_state: render_clickhouse_view(add_clickhouse(app_state)),
        [state],
        [state, clickhouse_list, clickhouse_name, clickhouse_endpoint, clickhouse_hooks, clickhouse_messages, clickhouse_timeout, clickhouse_status],
    )
    clickhouse_rename.click(
        lambda app_state, name: render_clickhouse_view(rename_clickhouse(app_state, name)),
        [state, clickhouse_name],
        [state, clickhouse_list, clickhouse_name, clickhouse_endpoint, clickhouse_hooks, clickhouse_messages, clickhouse_timeout, clickhouse_status],
    )
    clickhouse_delete.click(
        lambda app_state: render_clickhouse_view(delete_clickhouse(app_state)),
        [state],
        [state, clickhouse_list, clickhouse_name, clickhouse_endpoint, clickhouse_hooks, clickhouse_messages, clickhouse_timeout, clickhouse_status],
    )
    clickhouse_endpoint.change(save_clickhouse_form, [state, clickhouse_endpoint, clickhouse_hooks, clickhouse_messages, clickhouse_timeout], [state])
    clickhouse_hooks.change(save_clickhouse_form, [state, clickhouse_endpoint, clickhouse_hooks, clickhouse_messages, clickhouse_timeout], [state])
    clickhouse_messages.change(save_clickhouse_form, [state, clickhouse_endpoint, clickhouse_hooks, clickhouse_messages, clickhouse_timeout], [state])
    clickhouse_timeout.change(save_clickhouse_form, [state, clickhouse_endpoint, clickhouse_hooks, clickhouse_messages, clickhouse_timeout], [state])
    clickhouse_send.click(send_clickhouse, [state, clickhouse_endpoint, clickhouse_hooks, clickhouse_messages, clickhouse_timeout], [state, clickhouse_status])


def main() -> None:
    app.launch(css=APP_CSS, js=APP_JS)


if __name__ == "__main__":
    main()
