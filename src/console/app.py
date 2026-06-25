from pathlib import Path

import gradio as gr
from gradio_verticalusecaselist import VerticalUseCaseList

from console.converters.state_converter import load_state
from console.handlers import (
    clickhouse_panel_updates,
    kafka_panel_updates,
    load_all,
    on_autosave_tick,
    on_backup_tick,
    on_clickhouse_field_change,
    on_clickhouse_send,
    on_clickhouse_sidebar_add,
    on_clickhouse_sidebar_change,
    on_kafka_field_change,
    on_kafka_send,
    on_kafka_sidebar_add,
    on_kafka_sidebar_change,
    on_manual_backup,
    reload_from_disk,
    save_all,
)
from console.help_texts import build_main_help_columns
from console.parser import DIRECTIVE_CLICKHOUSE_TABLE, DIRECTIVE_TIMEOUT
from console.persistence import (
    AUTOSAVE_DEBOUNCE_SEC,
    BACKUP_INTERVAL_SEC,
    CONFIG_PATH,
    format_status,
    initial_save_meta,
)
from console.settings import ALLOW_USE_CASE_DELETE
from console.use_case_state import (
    build_clickhouse_sidebar_value,
    build_kafka_sidebar_value,
    clamp_index,
)


STATIC_DIR = Path(__file__).resolve().parent / "static"
APP_CSS = (STATIC_DIR / "app.css").read_text(encoding="utf-8")
APP_JS = (STATIC_DIR / "app.js").read_text(encoding="utf-8")


def on_allow_delete_toggle(enabled: bool) -> tuple[bool, dict, dict]:
    sidebar_update = gr.update(allow_delete=enabled)
    return enabled, sidebar_update, sidebar_update


def wire_config_actions(
    save_button: gr.Button,
    load_button: gr.Button,
    backup_button: gr.Button,
    *,
    state: gr.State,
    save_meta: gr.State,
    backup_last_at: gr.State,
    save_status: gr.Textbox,
    load_outputs: list,
) -> None:
    save_button.click(save_all, [state, save_meta], [state, save_meta, save_status])
    load_button.click(load_all, [state, save_meta], load_outputs)
    backup_button.click(on_manual_backup, [state, backup_last_at], [backup_last_at, save_status])


def create_app() -> gr.Blocks:
    initial_state = load_state(CONFIG_PATH)
    initial_save_meta_value = initial_save_meta(CONFIG_PATH.exists())
    initial_kafka = initial_state.kafka_use_cases[
        clamp_index(initial_state.selected_kafka_index, len(initial_state.kafka_use_cases))
    ]
    initial_clickhouse = initial_state.clickhouse_use_cases[
        clamp_index(initial_state.selected_clickhouse_index, len(initial_state.clickhouse_use_cases))
    ]

    with gr.Blocks(title="QA ClickHouse Kafka Helper", fill_width=True) as blocks:
        state = gr.State(initial_state)
        save_meta = gr.State(initial_save_meta_value)
        allow_delete_state = gr.State(ALLOW_USE_CASE_DELETE)
        backup_last_at = gr.State(None)
        kafka_logs_state = gr.State({})
        clickhouse_logs_state = gr.State({})

        with gr.Sidebar(
            label="Конфигурация",
            open=False,
            position="left",
            width=280,
            elem_id="config-sidebar",
            elem_classes=["config-sidebar-panel"],
        ) as config_sidebar:
            allow_delete_toggle = gr.Checkbox(
                label="Разрешить удаление",
                value=ALLOW_USE_CASE_DELETE,
            )
            save_button = gr.Button("Сохранить", elem_classes=["config-action-btn"])
            load_button = gr.Button("Загрузить", elem_classes=["config-action-btn"])
            backup_button = gr.Button("Бэкап", elem_classes=["config-action-btn"])

        gr.Markdown("## QA ClickHouse Kafka Helper")
        help_left, help_right = build_main_help_columns(DIRECTIVE_TIMEOUT, DIRECTIVE_CLICKHOUSE_TABLE)
        with gr.Row():
            with gr.Column(scale=1):
                gr.Markdown(help_left)
            with gr.Column(scale=1):
                gr.Markdown(help_right)

        with gr.Row(elem_id="config-status-row", equal_height=True):
            save_status = gr.Textbox(
                show_label=False,
                interactive=False,
                value=format_status(initial_save_meta_value),
                elem_id="save-status",
                container=False,
                lines=1,
                max_lines=1,
            )

        autosave_timer = gr.Timer(AUTOSAVE_DEBOUNCE_SEC, active=True)
        backup_timer = gr.Timer(BACKUP_INTERVAL_SEC, active=True)

        with gr.Tabs():
            with gr.Tab("Kafka"):
                kafka_status = gr.Textbox(label="Status", interactive=False, value="")
                with gr.Row():
                    with gr.Column(scale=1, min_width=280):
                        kafka_sidebar = VerticalUseCaseList(
                            value=build_kafka_sidebar_value(initial_state),
                            label="Use cases",
                            elem_id="kafka-use-cases",
                            allow_delete=ALLOW_USE_CASE_DELETE,
                        )
                    with gr.Column(scale=4):
                        kafka_url = gr.Textbox(label="Bootstrap URL", value=initial_kafka.bootstrap_url)
                        kafka_topic = gr.Textbox(label="Topic", value=initial_kafka.topic)
                        kafka_hooks = gr.Code(
                            label="Hooks", language="python", lines=10, value=initial_kafka.hooks, elem_id="kafka-hooks"
                        )
                        kafka_messages = gr.Code(
                            label="Messages", language="json", lines=14, value=initial_kafka.messages, elem_id="kafka-messages"
                        )
                        kafka_timeout = gr.Number(label="Global timeout (ms)", precision=0, value=initial_kafka.global_timeout_ms)
                        kafka_send = gr.Button("Send")
                        with gr.Accordion("Console logs", open=False):
                            kafka_console = gr.Textbox(label="Logs", lines=12, interactive=False, value="", elem_id="kafka-console")

                kafka_form_inputs = [
                    state,
                    kafka_logs_state,
                    save_meta,
                    kafka_url,
                    kafka_topic,
                    kafka_hooks,
                    kafka_messages,
                    kafka_timeout,
                ]
                kafka_form_outputs = [
                    state,
                    kafka_sidebar,
                    kafka_url,
                    kafka_topic,
                    kafka_hooks,
                    kafka_messages,
                    kafka_timeout,
                    kafka_console,
                ]

                kafka_sidebar.change(
                    on_kafka_sidebar_change,
                    [
                        kafka_sidebar,
                        state,
                        kafka_logs_state,
                        save_meta,
                        allow_delete_state,
                        kafka_url,
                        kafka_topic,
                        kafka_hooks,
                        kafka_messages,
                        kafka_timeout,
                    ],
                    [state, kafka_logs_state, save_meta, save_status, *kafka_form_outputs[1:]],
                )
                kafka_sidebar.add(
                    on_kafka_sidebar_add,
                    kafka_form_inputs,
                    [*kafka_form_outputs, kafka_status, save_meta, save_status],
                )
                kafka_send.click(
                    on_kafka_send,
                    kafka_form_inputs,
                    [state, kafka_logs_state, *kafka_form_outputs[1:], kafka_status, save_meta, save_status],
                )

                kafka_field_inputs = [state, save_meta, kafka_url, kafka_topic, kafka_hooks, kafka_messages, kafka_timeout]
                for field in [kafka_url, kafka_topic, kafka_hooks, kafka_messages, kafka_timeout]:
                    field.change(on_kafka_field_change, kafka_field_inputs, [state, save_meta, save_status])

            with gr.Tab("ClickHouse"):
                clickhouse_status = gr.Textbox(label="Status", interactive=False, value="")
                with gr.Row():
                    with gr.Column(scale=1, min_width=280):
                        clickhouse_sidebar = VerticalUseCaseList(
                            value=build_clickhouse_sidebar_value(initial_state),
                            label="Use cases",
                            elem_id="clickhouse-use-cases",
                            allow_delete=ALLOW_USE_CASE_DELETE,
                        )
                    with gr.Column(scale=4):
                        clickhouse_host = gr.Textbox(label="Host", value=initial_clickhouse.host)
                        clickhouse_port = gr.Number(label="Port", precision=0, value=initial_clickhouse.port)
                        clickhouse_user = gr.Textbox(label="User", value=initial_clickhouse.user)
                        clickhouse_password = gr.Textbox(
                            label="Password", type="password", value=initial_clickhouse.password
                        )
                        clickhouse_database = gr.Textbox(label="Database", value=initial_clickhouse.database)
                        clickhouse_hooks = gr.Code(
                            label="Hooks",
                            language="python",
                            lines=10,
                            value=initial_clickhouse.hooks,
                            elem_id="clickhouse-hooks",
                        )
                        clickhouse_messages = gr.Code(
                            label="Messages",
                            language="sql",
                            lines=14,
                            value=initial_clickhouse.messages,
                            elem_id="clickhouse-messages",
                        )
                        clickhouse_timeout = gr.Number(
                            label="Global timeout (ms)", precision=0, value=initial_clickhouse.global_timeout_ms
                        )
                        clickhouse_send = gr.Button("Send")
                        with gr.Accordion("Console logs", open=False):
                            clickhouse_console = gr.Textbox(
                                label="Logs", lines=12, interactive=False, value="", elem_id="clickhouse-console"
                            )

                clickhouse_form_inputs = [
                    state,
                    clickhouse_logs_state,
                    save_meta,
                    clickhouse_host,
                    clickhouse_port,
                    clickhouse_user,
                    clickhouse_password,
                    clickhouse_database,
                    clickhouse_hooks,
                    clickhouse_messages,
                    clickhouse_timeout,
                ]
                clickhouse_form_outputs = [
                    state,
                    clickhouse_sidebar,
                    clickhouse_host,
                    clickhouse_port,
                    clickhouse_user,
                    clickhouse_password,
                    clickhouse_database,
                    clickhouse_hooks,
                    clickhouse_messages,
                    clickhouse_timeout,
                    clickhouse_console,
                ]

                clickhouse_sidebar.change(
                    on_clickhouse_sidebar_change,
                    [
                        clickhouse_sidebar,
                        state,
                        clickhouse_logs_state,
                        save_meta,
                        allow_delete_state,
                        clickhouse_host,
                        clickhouse_port,
                        clickhouse_user,
                        clickhouse_password,
                        clickhouse_database,
                        clickhouse_hooks,
                        clickhouse_messages,
                        clickhouse_timeout,
                    ],
                    [state, clickhouse_logs_state, save_meta, save_status, *clickhouse_form_outputs[1:]],
                )
                clickhouse_sidebar.add(
                    on_clickhouse_sidebar_add,
                    clickhouse_form_inputs,
                    [*clickhouse_form_outputs, clickhouse_status, save_meta, save_status],
                )
                clickhouse_send.click(
                    on_clickhouse_send,
                    clickhouse_form_inputs,
                    [state, clickhouse_logs_state, *clickhouse_form_outputs[1:], clickhouse_status, save_meta, save_status],
                )

                clickhouse_field_inputs = [
                    state,
                    save_meta,
                    clickhouse_host,
                    clickhouse_port,
                    clickhouse_user,
                    clickhouse_password,
                    clickhouse_database,
                    clickhouse_hooks,
                    clickhouse_messages,
                    clickhouse_timeout,
                ]
                for field in [
                    clickhouse_host,
                    clickhouse_port,
                    clickhouse_user,
                    clickhouse_password,
                    clickhouse_database,
                    clickhouse_hooks,
                    clickhouse_messages,
                    clickhouse_timeout,
                ]:
                    field.change(on_clickhouse_field_change, clickhouse_field_inputs, [state, save_meta, save_status])

        load_outputs = [
            state,
            save_meta,
            save_status,
            kafka_logs_state,
            clickhouse_logs_state,
            kafka_status,
            clickhouse_status,
            kafka_sidebar,
            kafka_url,
            kafka_topic,
            kafka_hooks,
            kafka_messages,
            kafka_timeout,
            kafka_console,
            clickhouse_sidebar,
            clickhouse_host,
            clickhouse_port,
            clickhouse_user,
            clickhouse_password,
            clickhouse_database,
            clickhouse_hooks,
            clickhouse_messages,
            clickhouse_timeout,
            clickhouse_console,
        ]
        wire_config_actions(
            save_button,
            load_button,
            backup_button,
            state=state,
            save_meta=save_meta,
            backup_last_at=backup_last_at,
            save_status=save_status,
            load_outputs=load_outputs,
        )
        allow_delete_toggle.change(
            on_allow_delete_toggle,
            [allow_delete_toggle],
            [allow_delete_state, kafka_sidebar, clickhouse_sidebar],
        )

        blocks.load(reload_from_disk, [], load_outputs)
        autosave_timer.tick(on_autosave_tick, [state, save_meta], [save_meta, save_status])
        backup_timer.tick(on_backup_tick, [state, backup_last_at], [backup_last_at])

    return blocks


def main() -> None:
    create_app().launch(css=APP_CSS, js=APP_JS)


if __name__ == "__main__":
    main()
