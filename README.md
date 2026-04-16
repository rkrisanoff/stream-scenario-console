# Stream Scenario Console

`Stream Scenario Console` — минималистичный QA-инструмент для прогона сценариев событий:

- отправка сообщений в Kafka;
- отправка JSON и SQL в ClickHouse;
- use cases (наборы сценариев) с сохранением в `configuration.json`;
- директивы в сообщениях (`@timeout`, `@clickhouse.table`);
- hooks как обычный Python-скрипт для генерации данных (`uuid4`, `datetime`, арифметика и т.д.).

## Быстрый запуск

### Вариант 1: через `uv`

```bash
uv sync
uv run python ./app.py
```

### Вариант 2: через `pip`

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python ./app.py
```

После запуска открой локальный URL из терминала (обычно `http://127.0.0.1:7860`).

## Тесты

```bash
uv run pytest --cov . -v
```
