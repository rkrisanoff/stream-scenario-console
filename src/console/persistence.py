from dataclasses import dataclass, replace
from datetime import datetime
from pathlib import Path

from console.converters.state_converter import dump_state
from console.dtos import AppStateDto


CONFIG_PATH = Path("configuration.json")
BACKUP_DIR = Path("configuration.backups")
AUTOSAVE_DEBOUNCE_SEC = 2
BACKUP_INTERVAL_SEC = 1800
MAX_BACKUPS = 10


@dataclass(slots=True)
class SaveMeta:
    dirty: bool
    last_saved_at: datetime | None
    last_message: str


def format_time(saved_at: datetime) -> str:
    return f"{saved_at:%H:%M:%S}.{saved_at.microsecond // 1000:03d}"


def format_status(meta: SaveMeta) -> str:
    if meta.last_message.startswith("Ошибка сохранения"):
        return meta.last_message
    if meta.dirty:
        return "● Несохранённые изменения"
    if meta.last_message == "loaded":
        return "Загружено из configuration.json"
    if meta.last_saved_at is None:
        return "Не сохранено"
    if meta.last_message == "manual":
        return f"Сохранено вручную {format_time(meta.last_saved_at)}"
    if meta.last_message == "autosave":
        return f"Автосохранено {format_time(meta.last_saved_at)}"
    return f"Сохранено {format_time(meta.last_saved_at)}"


def initial_save_meta(config_exists: bool) -> SaveMeta:
    if config_exists:
        return SaveMeta(dirty=False, last_saved_at=None, last_message="loaded")
    return SaveMeta(dirty=False, last_saved_at=None, last_message="")


def mark_dirty(meta: SaveMeta) -> SaveMeta:
    return replace(meta, dirty=True)


def save_configuration(
    config_path: Path,
    state: AppStateDto,
    meta: SaveMeta,
    *,
    message: str = "autosave",
) -> SaveMeta:
    try:
        dump_state(config_path, state)
        return SaveMeta(dirty=False, last_saved_at=datetime.now(), last_message=message)
    except Exception as exc:
        return replace(meta, dirty=True, last_message=f"Ошибка сохранения: {exc}")


def save_if_dirty(config_path: Path, state: AppStateDto, meta: SaveMeta) -> SaveMeta:
    if not meta.dirty:
        return meta
    return save_configuration(config_path, state, meta, message="autosave")


def immediate_save(
    config_path: Path,
    state: AppStateDto,
    meta: SaveMeta,
    *,
    message: str = "autosave",
) -> tuple[SaveMeta, str]:
    new_meta = save_configuration(config_path, state, meta, message=message)
    return new_meta, format_status(new_meta)


def create_rotating_backup(state: AppStateDto, backup_dir: Path, max_backups: int) -> Path:
    backup_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    backup_path = backup_dir / f"configuration-{timestamp}.json"
    dump_state(backup_path, state)
    backups = sorted(backup_dir.glob("configuration-*.json"), key=lambda path: path.stat().st_mtime)
    while len(backups) > max_backups:
        backups.pop(0).unlink()
    return backup_path


def should_run_scheduled_backup(last_backup_at: datetime | None, interval_sec: int) -> bool:
    if last_backup_at is None:
        return True
    return (datetime.now() - last_backup_at).total_seconds() >= interval_sec


def run_backup(state: AppStateDto) -> Path:
    return create_rotating_backup(state, BACKUP_DIR, MAX_BACKUPS)


def format_backup_status(backup_path: Path) -> str:
    return f"Бэкап {backup_path.name}"
