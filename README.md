# Stream Scenario Console

`Stream Scenario Console` — минималистичный QA-инструмент для прогона сценариев событий:

- отправка сообщений в Kafka;
- отправка JSON и SQL в ClickHouse;
- use cases (наборы сценариев) с сохранением в `configuration.json`;
- директивы в сообщениях (`@timeout`, `@clickhouse.table`);
- hooks как обычный Python-скрипт для генерации данных (`uuid4`, `datetime`, арифметика и т.д.).

## Установка uv (Astral)

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

Проверка:

```bash
uv --version
```

## Установка проекта

### Вариант A: через `uv sync` (рекомендуется)

```bash
uv sync
```

### Вариант B

```bash
uv venv .venv --python=3.14
source .venv/bin/activate
uv pip install --editable .
uv pip install --group dev --editable .
```

## Запуск приложения

```bash
uv run run-app
```

После запуска открой локальный URL из терминала (обычно `http://127.0.0.1:7860`).