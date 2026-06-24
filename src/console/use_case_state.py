from dataclasses import replace

from console.converters.state_converter import (
    load_default_clickhouse_use_case,
    load_default_kafka_use_case,
)
from console.dtos import AppStateDto


type LogsByUseCase = dict[str, str]
type SidebarValue = dict[str, object]


def clamp_index(index: int, length: int) -> int:
    if length <= 0:
        return 0
    return max(0, min(index, length - 1))


def clone_state(state: AppStateDto) -> AppStateDto:
    kafka_use_cases = [replace(item) for item in state.kafka_use_cases]
    clickhouse_use_cases = [replace(item) for item in state.clickhouse_use_cases]
    return AppStateDto(
        kafka_use_cases=kafka_use_cases,
        clickhouse_use_cases=clickhouse_use_cases,
        selected_kafka_index=state.selected_kafka_index,
        selected_clickhouse_index=state.selected_clickhouse_index,
    )


def _find_index_by_id(use_cases: list, use_case_id: str) -> int | None:
    for index, use_case in enumerate(use_cases):
        if use_case.id == use_case_id:
            return index
    return None


def find_kafka_index_by_id(state: AppStateDto, use_case_id: str) -> int | None:
    return _find_index_by_id(state.kafka_use_cases, use_case_id)


def find_clickhouse_index_by_id(state: AppStateDto, use_case_id: str) -> int | None:
    return _find_index_by_id(state.clickhouse_use_cases, use_case_id)


def _build_sidebar_value(use_cases: list, selected_index: int) -> SidebarValue:
    index = clamp_index(selected_index, len(use_cases))
    selected = use_cases[index]
    return {
        "items": [{"id": item.id, "name": item.name} for item in use_cases],
        "selected_id": selected.id,
    }


def build_kafka_sidebar_value(state: AppStateDto) -> SidebarValue:
    return _build_sidebar_value(state.kafka_use_cases, state.selected_kafka_index)


def build_clickhouse_sidebar_value(state: AppStateDto) -> SidebarValue:
    return _build_sidebar_value(state.clickhouse_use_cases, state.selected_clickhouse_index)


def _reorder_use_cases(
    state: AppStateDto,
    ordered_ids: list[str],
    cases_attr: str,
    selected_attr: str,
) -> AppStateDto:
    if not ordered_ids:
        return state
    next_state = clone_state(state)
    use_cases = getattr(next_state, cases_attr)
    by_id = {item.id: item for item in use_cases}
    if set(by_id.keys()) != set(ordered_ids):
        return state
    selected_index = clamp_index(getattr(next_state, selected_attr), len(use_cases))
    selected_id = use_cases[selected_index].id
    setattr(next_state, cases_attr, [by_id[item_id] for item_id in ordered_ids])
    setattr(next_state, selected_attr, ordered_ids.index(selected_id) if selected_id in ordered_ids else 0)
    return next_state


def reorder_kafka_use_cases(state: AppStateDto, ordered_ids: list[str]) -> AppStateDto:
    return _reorder_use_cases(state, ordered_ids, "kafka_use_cases", "selected_kafka_index")


def reorder_clickhouse_use_cases(state: AppStateDto, ordered_ids: list[str]) -> AppStateDto:
    return _reorder_use_cases(state, ordered_ids, "clickhouse_use_cases", "selected_clickhouse_index")


def _apply_sidebar_value(
    state: AppStateDto,
    sidebar_value: SidebarValue,
    cases_attr: str,
    selected_attr: str,
) -> AppStateDto:
    raw_items = sidebar_value.get("items", [])
    if not isinstance(raw_items, list):
        return clone_state(state)

    ordered_ids: list[str] = []
    name_by_id: dict[str, str] = {}
    for item in raw_items:
        if not isinstance(item, dict) or item.get("id") is None:
            continue
        item_id = str(item["id"])
        ordered_ids.append(item_id)
        if "name" in item:
            name_by_id[item_id] = str(item["name"])

    if not ordered_ids:
        return state

    use_cases = getattr(state, cases_attr)
    by_id = {item.id: item for item in use_cases}
    if not set(ordered_ids).issubset(by_id.keys()):
        return state

    next_state = clone_state(state)
    new_use_cases = [by_id[item_id] for item_id in ordered_ids]
    for item_id, name in name_by_id.items():
        index = _find_index_by_id(new_use_cases, item_id)
        if index is not None:
            new_use_cases[index].name = name
    setattr(next_state, cases_attr, new_use_cases)

    selected_id = str(sidebar_value.get("selected_id", ""))
    index = _find_index_by_id(new_use_cases, selected_id)
    if index is not None:
        setattr(next_state, selected_attr, index)
    else:
        current = getattr(next_state, selected_attr)
        setattr(next_state, selected_attr, clamp_index(current, len(new_use_cases)))
    return next_state


def apply_kafka_sidebar_value(state: AppStateDto, sidebar_value: SidebarValue) -> AppStateDto:
    return _apply_sidebar_value(state, sidebar_value, "kafka_use_cases", "selected_kafka_index")


def apply_clickhouse_sidebar_value(state: AppStateDto, sidebar_value: SidebarValue) -> AppStateDto:
    return _apply_sidebar_value(state, sidebar_value, "clickhouse_use_cases", "selected_clickhouse_index")


def select_kafka_index(state: AppStateDto, index: int) -> AppStateDto:
    next_state = clone_state(state)
    next_state.selected_kafka_index = clamp_index(index, len(next_state.kafka_use_cases))
    return next_state


def select_clickhouse_index(state: AppStateDto, index: int) -> AppStateDto:
    next_state = clone_state(state)
    next_state.selected_clickhouse_index = clamp_index(index, len(next_state.clickhouse_use_cases))
    return next_state


def add_kafka_use_case(state: AppStateDto) -> tuple[AppStateDto, str]:
    next_state = clone_state(state)
    use_case_name = f"Kafka use case {len(next_state.kafka_use_cases) + 1}"
    next_state.kafka_use_cases.append(load_default_kafka_use_case(use_case_name))
    next_state.selected_kafka_index = len(next_state.kafka_use_cases) - 1
    return next_state, "ok: use case added"


def add_clickhouse_use_case(state: AppStateDto) -> tuple[AppStateDto, str]:
    next_state = clone_state(state)
    use_case_name = f"ClickHouse use case {len(next_state.clickhouse_use_cases) + 1}"
    next_state.clickhouse_use_cases.append(load_default_clickhouse_use_case(use_case_name))
    next_state.selected_clickhouse_index = len(next_state.clickhouse_use_cases) - 1
    return next_state, "ok: use case added"


def rename_selected_kafka_use_case(state: AppStateDto, new_name: str) -> tuple[AppStateDto, str]:
    name = new_name.strip()
    if not name:
        return state, "error: empty use case name"
    next_state = clone_state(state)
    selected_index = clamp_index(next_state.selected_kafka_index, len(next_state.kafka_use_cases))
    next_state.kafka_use_cases[selected_index].name = name
    return next_state, "ok: renamed"


def rename_selected_clickhouse_use_case(state: AppStateDto, new_name: str) -> tuple[AppStateDto, str]:
    name = new_name.strip()
    if not name:
        return state, "error: empty use case name"
    next_state = clone_state(state)
    selected_index = clamp_index(next_state.selected_clickhouse_index, len(next_state.clickhouse_use_cases))
    next_state.clickhouse_use_cases[selected_index].name = name
    return next_state, "ok: renamed"


def _delete_use_case(
    state: AppStateDto,
    use_case_id: str,
    logs_by_use_case: LogsByUseCase,
    cases_attr: str,
    selected_attr: str,
) -> tuple[AppStateDto, LogsByUseCase, str]:
    use_cases = getattr(state, cases_attr)
    if len(use_cases) <= 1:
        return state, logs_by_use_case, "error: cannot delete last use case"
    index = _find_index_by_id(use_cases, use_case_id)
    if index is None:
        return state, logs_by_use_case, "error: use case not found"
    next_state = clone_state(state)
    next_use_cases = getattr(next_state, cases_attr)
    deleted_id = next_use_cases[index].id
    next_use_cases.pop(index)
    selected_index = clamp_index(getattr(state, selected_attr), len(use_cases))
    if selected_index == index:
        setattr(next_state, selected_attr, clamp_index(index, len(next_use_cases)))
    elif selected_index > index:
        setattr(next_state, selected_attr, selected_index - 1)
    else:
        setattr(next_state, selected_attr, selected_index)
    updated_logs = dict(logs_by_use_case)
    updated_logs.pop(deleted_id, None)
    return next_state, updated_logs, "ok: deleted"


def delete_kafka_use_case(
    state: AppStateDto,
    use_case_id: str,
    logs_by_use_case: LogsByUseCase,
) -> tuple[AppStateDto, LogsByUseCase, str]:
    return _delete_use_case(state, use_case_id, logs_by_use_case, "kafka_use_cases", "selected_kafka_index")


def delete_clickhouse_use_case(
    state: AppStateDto,
    use_case_id: str,
    logs_by_use_case: LogsByUseCase,
) -> tuple[AppStateDto, LogsByUseCase, str]:
    return _delete_use_case(state, use_case_id, logs_by_use_case, "clickhouse_use_cases", "selected_clickhouse_index")


def delete_selected_kafka_use_case(state: AppStateDto, logs_by_use_case: LogsByUseCase) -> tuple[AppStateDto, LogsByUseCase, str]:
    selected_index = clamp_index(state.selected_kafka_index, len(state.kafka_use_cases))
    use_case_id = state.kafka_use_cases[selected_index].id
    return delete_kafka_use_case(state, use_case_id, logs_by_use_case)


def delete_selected_clickhouse_use_case(state: AppStateDto, logs_by_use_case: LogsByUseCase) -> tuple[AppStateDto, LogsByUseCase, str]:
    selected_index = clamp_index(state.selected_clickhouse_index, len(state.clickhouse_use_cases))
    use_case_id = state.clickhouse_use_cases[selected_index].id
    return delete_clickhouse_use_case(state, use_case_id, logs_by_use_case)


def update_kafka_use_case_form(
    state: AppStateDto,
    use_case_id: str,
    bootstrap_url: str,
    topic: str,
    hooks: str,
    messages: str,
    global_timeout_ms: float,
) -> AppStateDto:
    next_state = clone_state(state)
    index = find_kafka_index_by_id(next_state, use_case_id)
    if index is None:
        return next_state
    use_case = next_state.kafka_use_cases[index]
    use_case.bootstrap_url = bootstrap_url.strip()
    use_case.topic = topic
    use_case.hooks = hooks
    use_case.messages = messages
    use_case.global_timeout_ms = int(global_timeout_ms)
    next_state.selected_kafka_index = index
    return next_state


def update_clickhouse_use_case_form(
    state: AppStateDto,
    use_case_id: str,
    host: str,
    port: float,
    user: str,
    password: str,
    database: str,
    hooks: str,
    messages: str,
    global_timeout_ms: float,
) -> AppStateDto:
    next_state = clone_state(state)
    index = find_clickhouse_index_by_id(next_state, use_case_id)
    if index is None:
        return next_state
    use_case = next_state.clickhouse_use_cases[index]
    use_case.host = host.strip()
    use_case.port = int(port)
    use_case.user = user.strip()
    use_case.password = password
    use_case.database = database.strip()
    use_case.hooks = hooks
    use_case.messages = messages
    use_case.global_timeout_ms = int(global_timeout_ms)
    next_state.selected_clickhouse_index = index
    return next_state


def persist_kafka_form(
    state: AppStateDto,
    bootstrap_url: str,
    topic: str,
    hooks: str,
    messages: str,
    global_timeout_ms: float,
) -> AppStateDto:
    selected_index = clamp_index(state.selected_kafka_index, len(state.kafka_use_cases))
    use_case_id = state.kafka_use_cases[selected_index].id
    return update_kafka_use_case_form(
        state,
        use_case_id,
        bootstrap_url,
        topic,
        hooks,
        messages,
        global_timeout_ms,
    )


def persist_clickhouse_form(
    state: AppStateDto,
    host: str,
    port: float,
    user: str,
    password: str,
    database: str,
    hooks: str,
    messages: str,
    global_timeout_ms: float,
) -> AppStateDto:
    selected_index = clamp_index(state.selected_clickhouse_index, len(state.clickhouse_use_cases))
    use_case_id = state.clickhouse_use_cases[selected_index].id
    return update_clickhouse_use_case_form(
        state,
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
