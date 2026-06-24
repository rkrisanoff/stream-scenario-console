import json
from pathlib import Path

from console.converters.state_converter import dump_state, load_default_state, load_state_from_json
from console.persistence import (
    SaveMeta,
    create_rotating_backup,
    format_status,
    format_time,
    immediate_save,
    initial_save_meta,
    mark_dirty,
    save_configuration,
    save_if_dirty,
)


def test_save_configuration_writes_valid_json(tmp_path: Path) -> None:
    config_path = tmp_path / "configuration.json"
    state = load_default_state()
    meta = SaveMeta(dirty=True, last_saved_at=None, last_message="")

    new_meta = save_configuration(config_path, state, meta)

    assert config_path.exists()
    assert new_meta.dirty is False
    assert new_meta.last_saved_at is not None
    assert new_meta.last_message == "autosave"
    payload = json.loads(config_path.read_text(encoding="utf-8"))
    assert "kafka_use_cases" in payload
    assert "clickhouse_use_cases" in payload


def test_save_if_dirty_skips_clean_state(tmp_path: Path) -> None:
    config_path = tmp_path / "configuration.json"
    state = load_default_state()
    meta = SaveMeta(dirty=False, last_saved_at=None, last_message="loaded")

    new_meta = save_if_dirty(config_path, state, meta)

    assert not config_path.exists()
    assert new_meta is meta


def test_save_if_dirty_writes_when_dirty(tmp_path: Path) -> None:
    config_path = tmp_path / "configuration.json"
    state = load_default_state()
    meta = mark_dirty(SaveMeta(dirty=False, last_saved_at=None, last_message="loaded"))

    new_meta = save_if_dirty(config_path, state, meta)

    assert config_path.exists()
    assert new_meta.dirty is False


def test_create_rotating_backup_prunes_old_files(tmp_path: Path) -> None:
    backup_dir = tmp_path / "configuration.backups"
    backup_dir.mkdir()
    state = load_default_state()
    for index in range(12):
        dump_state(backup_dir / f"configuration-20250619-12000{index}.json", state)

    create_rotating_backup(state, backup_dir, max_backups=10)

    assert len(list(backup_dir.glob("configuration-*.json"))) == 10


def test_format_status_messages() -> None:
    from datetime import datetime

    saved_at = datetime(2026, 6, 19, 14, 32)

    assert format_status(initial_save_meta(True)) == "Загружено из configuration.json"
    assert format_status(mark_dirty(SaveMeta(False, saved_at, "autosave"))) == "● Несохранённые изменения"
    assert format_status(SaveMeta(False, saved_at, "autosave")) == "Автосохранено 14:32:00.000"
    assert format_status(SaveMeta(False, saved_at, "manual")) == "Сохранено вручную 14:32:00.000"
    assert format_status(SaveMeta(True, None, "Ошибка сохранения: disk full")).startswith("Ошибка сохранения")


def test_reload_from_disk_reads_latest_configuration(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    from console.handlers import reload_from_disk

    config_path = tmp_path / "configuration.json"
    state = load_default_state()
    state.kafka_use_cases[0].name = "Renamed on disk"
    save_configuration(config_path, state, SaveMeta(dirty=True, last_saved_at=None, last_message=""))

    result = reload_from_disk()

    assert result[0].kafka_use_cases[0].name == "Renamed on disk"
    assert result[2] == "Загружено из configuration.json"


def test_roundtrip_through_saved_file(tmp_path: Path) -> None:
    config_path = tmp_path / "configuration.json"
    original = load_default_state()
    original.kafka_use_cases[0].topic = "events-roundtrip"

    save_configuration(config_path, original, SaveMeta(dirty=True, last_saved_at=None, last_message=""))

    loaded = load_state_from_json(config_path.read_text(encoding="utf-8"))
    assert loaded.kafka_use_cases[0].topic == "events-roundtrip"


def test_format_time_includes_milliseconds() -> None:
    from datetime import datetime

    assert format_time(datetime(2026, 6, 19, 14, 32, 5, 123456)) == "14:32:05.123"


def test_initial_save_meta_without_file() -> None:
    meta = initial_save_meta(False)
    assert meta.dirty is False
    assert meta.last_message == ""


def test_immediate_save_writes_file(tmp_path: Path) -> None:
    config_path = tmp_path / "configuration.json"
    state = load_default_state()
    meta = SaveMeta(dirty=True, last_saved_at=None, last_message="")

    new_meta, status = immediate_save(config_path, state, meta, message="manual")

    assert config_path.exists()
    assert new_meta.dirty is False
    assert "вручную" in status
