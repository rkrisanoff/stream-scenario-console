from pathlib import Path

import pytest

from console.converters.state_converter import dump_state, load_default_state, load_state_from_json
from console.handlers import (
    load_all,
    on_autosave_tick,
    on_backup_tick,
    on_manual_backup,
    reload_from_disk,
    save_all,
)
from console.persistence import (
    CONFIG_PATH,
    SaveMeta,
    immediate_save,
    mark_dirty,
    save_configuration,
)


CLEAN_META = SaveMeta(dirty=False, last_saved_at=None, last_message="loaded")
DIRTY_META = mark_dirty(CLEAN_META)


@pytest.fixture
def isolated_cwd(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.chdir(tmp_path)
    return tmp_path


def test_save_all_writes_configuration(isolated_cwd: Path) -> None:
    state = load_default_state()
    state.kafka_use_cases[0].topic = "manual-save-topic"

    _, new_meta, status = save_all(state, DIRTY_META)

    assert CONFIG_PATH.exists()
    assert new_meta.dirty is False
    assert new_meta.last_message == "manual"
    assert "вручную" in status
    loaded = load_state_from_json(CONFIG_PATH.read_text(encoding="utf-8"))
    assert loaded.kafka_use_cases[0].topic == "manual-save-topic"


def test_save_all_keeps_state_unchanged(isolated_cwd: Path) -> None:
    state = load_default_state()
    returned_state, _, _ = save_all(state, DIRTY_META)
    assert returned_state is state


def test_on_autosave_tick_skips_clean(isolated_cwd: Path) -> None:
    state = load_default_state()
    new_meta, status = on_autosave_tick(state, CLEAN_META)
    assert new_meta is CLEAN_META
    assert status == "Загружено из configuration.json"
    assert not CONFIG_PATH.exists()


def test_on_autosave_tick_writes_dirty(isolated_cwd: Path) -> None:
    state = load_default_state()
    state.kafka_use_cases[0].topic = "autosaved"

    new_meta, status = on_autosave_tick(state, DIRTY_META)

    assert CONFIG_PATH.exists()
    assert new_meta.dirty is False
    assert new_meta.last_message == "autosave"
    assert "Автосохранено" in status


def test_reload_from_disk_clears_logs(isolated_cwd: Path) -> None:
    save_configuration(CONFIG_PATH, load_default_state(), DIRTY_META)
    result = reload_from_disk()
    assert result[3] == {}
    assert result[4] == {}


def test_reload_from_disk_resets_statuses(isolated_cwd: Path) -> None:
    save_configuration(CONFIG_PATH, load_default_state(), DIRTY_META)
    result = reload_from_disk()
    assert result[5] == ""
    assert result[6] == ""


def test_load_all_discards_dirty_and_reads_disk(isolated_cwd: Path) -> None:
    state = load_default_state()
    state.kafka_use_cases[0].topic = "dirty-in-memory"
    other = load_default_state()
    other.kafka_use_cases[0].topic = "on-disk"
    save_configuration(CONFIG_PATH, other, DIRTY_META)

    result = load_all(state, DIRTY_META)

    assert result[0].kafka_use_cases[0].topic == "on-disk"
    on_disk = load_state_from_json(CONFIG_PATH.read_text(encoding="utf-8"))
    assert on_disk.kafka_use_cases[0].topic == "on-disk"


def test_load_all_returns_panel_updates(isolated_cwd: Path) -> None:
    state = load_default_state()
    state.kafka_use_cases[0].bootstrap_url = "disk:9092"
    save_configuration(CONFIG_PATH, state, DIRTY_META)

    result = load_all(load_default_state(), CLEAN_META)

    assert result[8]["value"] == "disk:9092"


def test_on_backup_tick_creates_backup_file(isolated_cwd: Path) -> None:
    state = load_default_state()
    on_backup_tick(state, None)
    backups = list(Path("configuration.backups").glob("configuration-*.json"))
    assert len(backups) == 1


def test_on_backup_tick_skips_until_interval_elapsed(isolated_cwd: Path) -> None:
    from datetime import datetime

    state = load_default_state()
    last_at = on_backup_tick(state, None)
    assert last_at is not None
    assert len(list(Path("configuration.backups").glob("configuration-*.json"))) == 1

    on_backup_tick(state, last_at)
    assert len(list(Path("configuration.backups").glob("configuration-*.json"))) == 1


def test_on_manual_backup_creates_file_and_resets_schedule(isolated_cwd: Path) -> None:
    state = load_default_state()
    last_at, status = on_manual_backup(state, None)

    assert last_at is not None
    assert status.startswith("Бэкап configuration-")
    assert len(list(Path("configuration.backups").glob("configuration-*.json"))) == 1

    on_backup_tick(state, last_at)
    assert len(list(Path("configuration.backups").glob("configuration-*.json"))) == 1


def test_on_backup_tick_rotates_old_backups(isolated_cwd: Path) -> None:
    backup_dir = Path("configuration.backups")
    backup_dir.mkdir()
    state = load_default_state()
    for index in range(12):
        dump_state(backup_dir / f"configuration-20250619-12000{index}.json", state)

    on_backup_tick(state, None)

    assert len(list(backup_dir.glob("configuration-*.json"))) == 10


def test_immediate_save_returns_status_text(isolated_cwd: Path) -> None:
    state = load_default_state()
    new_meta, status = immediate_save(CONFIG_PATH, state, DIRTY_META, message="manual")
    assert new_meta.dirty is False
    assert "вручную" in status


def test_sidebar_change_writes_immediately(isolated_cwd: Path) -> None:
    from console.handlers import on_kafka_sidebar_change
    from console.use_case_state import add_kafka_use_case, build_kafka_sidebar_value

    state = load_default_state()
    state, _ = add_kafka_use_case(state)
    first, second = state.kafka_use_cases
    sidebar = build_kafka_sidebar_value(state)
    sidebar["selected_id"] = second.id

    on_kafka_sidebar_change(
        sidebar,
        state,
        {},
        CLEAN_META,
        True,
        first.bootstrap_url,
        first.topic,
        first.hooks,
        first.messages,
        float(first.global_timeout_ms),
    )

    assert CONFIG_PATH.exists()


def test_field_change_does_not_write_until_autosave(isolated_cwd: Path) -> None:
    from console.handlers import on_kafka_field_change

    state = load_default_state()
    uc = state.kafka_use_cases[0]
    new_state, new_meta, status = on_kafka_field_change(
        state,
        CLEAN_META,
        "new-bootstrap:9092",
        uc.topic,
        uc.hooks,
        uc.messages,
        float(uc.global_timeout_ms),
    )

    assert not CONFIG_PATH.exists()
    assert new_meta.dirty is True
    assert status == "● Несохранённые изменения"
    assert new_state.kafka_use_cases[0].bootstrap_url == "new-bootstrap:9092"
