from typing import Any

from gradio.components.base import Component
from gradio.events import Events


type UseCaseListItem = dict[str, str]
type UseCaseListValue = dict[str, Any]


def normalize_use_case_list_value(payload: UseCaseListValue | None) -> UseCaseListValue:
    if not isinstance(payload, dict):
        return {"items": [], "selected_id": ""}
    raw_items = payload.get("items", [])
    items: list[UseCaseListItem] = []
    if isinstance(raw_items, list):
        for item in raw_items:
            if not isinstance(item, dict):
                continue
            item_id = item.get("id")
            if item_id is None:
                continue
            items.append({"id": str(item_id), "name": str(item.get("name", ""))})
    selected_id = str(payload.get("selected_id", ""))
    return {"items": items, "selected_id": selected_id}


class VerticalUseCaseList(Component):
    EVENTS = [Events.change, Events.select, "add"]

    def __init__(
        self,
        value: Any = None,
        *,
        allow_delete: bool = False,
        label: str | None = None,
        info: str | None = None,
        show_label: bool | None = None,
        container: bool = True,
        scale: int | None = None,
        min_width: int | None = None,
        interactive: bool | None = None,
        visible: bool = True,
        elem_id: str | None = None,
        elem_classes: list[str] | str | None = None,
        render: bool = True,
        key: int | str | tuple[int | str, ...] | None = None,
        preserved_by_key: list[str] | str | None = "value",
        load_fn=None,
        every=None,
        inputs=None,
    ):
        super().__init__(
            value,
            label=label,
            info=info,
            show_label=show_label,
            container=container,
            scale=scale,
            min_width=min_width,
            interactive=interactive,
            visible=visible,
            elem_id=elem_id,
            elem_classes=elem_classes,
            render=render,
            key=key,
            preserved_by_key=preserved_by_key,
            load_fn=load_fn,
            every=every,
            inputs=inputs,
        )
        self.allow_delete = allow_delete

    def preprocess(self, payload: UseCaseListValue | None) -> UseCaseListValue:
        return normalize_use_case_list_value(payload)

    def postprocess(self, value: UseCaseListValue | None) -> UseCaseListValue:
        return normalize_use_case_list_value(value)

    def example_payload(self) -> UseCaseListValue:
        return {"items": [{"id": "example-1", "name": "Example use case"}], "selected_id": "example-1"}

    def example_value(self) -> UseCaseListValue:
        return self.example_payload()

    def api_info(self) -> dict[str, Any]:
        return {"type": {}, "description": "object with items[{id,name}] and selected_id"}
