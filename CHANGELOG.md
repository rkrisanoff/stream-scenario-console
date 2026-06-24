# Changelog

Формат: [Keep a Changelog](https://keepachangelog.com/ru/1.1.0/), версии: [Semantic Versioning](https://semver.org/lang/ru/).

## [0.2.1] — 2026-06-24

### Добавлено

- Выдвижная панель **`gr.Sidebar`** слева: **Сохранить**, **Загрузить**, **Бэкап**, **Разрешить удаление**.
- Кнопка **Конфигурация** в строке статуса (открывает sidebar).
- Ручной **Бэкап** со сбросом таймера автоматического снимка (ещё 30 мин до следующего).

### Изменено

- Строка статуса — на всю ширину между шпаргалкой и вкладками; кнопки конфигурации убраны из верхнего тулбара.
- Колонка use case — только список сценариев, без кнопок.
- README: структура репозитория, таблицы UI/сохранения, список тестов.

### Исправлено

- **Загрузить** — только чтение `configuration.json` с диска, без предварительной записи несохранённых правок (откат из бэкапа снова работает).

## [0.2.0] — 2026-06-16

### Добавлено

- Кастомный Gradio-компонент `VerticalUseCaseList`: вертикальный sidebar (выбор, rename, DnD, add, delete при разрешении).
- Автосохранение: immediate при sidebar/send; debounce ~2 с при правке формы.
- Периодические бэкапы в `configuration.backups/` (30 мин, max 10 файлов).
- Модули `handlers.py`, `persistence.py`, `settings.py`.
- Серверная блокировка удаления use case (`guard_sidebar_deletion`).
- Функциональные тесты handlers, persistence, state; `pytest-asyncio`.

### Изменено

- UI: use case перенесены из полей формы в sidebar.
- F5 / `app.load` — чтение актуального `configuration.json` с диска.
- `app.py` → `create_app()`; логика в `handlers.py`.
- `use_case_state.py` — общие helpers Kafka/ClickHouse.
- `configuration.json`, `configuration.backups/` в `.gitignore`.

### Исправлено

- Порядок аргументов sidebar handlers (`sidebar_value` первым).
- Удаление use case через событие `change` компонента.

### Удалено

- `analogies.py`, неиспользуемые `KafkaViewDto` / `ClickHouseViewDto` / `LoadViewDto`.

## [0.1.0] — 2026-06-12

### Добавлено

- Gradio-консоль для Kafka и ClickHouse.
- Парсер, hooks, директивы `@timeout` / `@clickhouse.table`.
- `ScenarioRunner`, `configuration.json`.
- Базовые тесты parser и runner.
