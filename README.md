# Stream Scenario Console

`Stream Scenario Console` — минималистичный QA-инструмент для прогона сценариев событий:

- отправка сообщений в Kafka;
- отправка JSON и SQL в ClickHouse;
- use cases (наборы сценариев) с сохранением в `configuration.json`;
- директивы в сообщениях (`@timeout`, `@clickhouse.table`);
- hooks как обычный Python-скрипт для генерации данных (`uuid4`, `datetime`, арифметика и т.д.).

Стек: Python 3.14, Gradio 6, кастомный компонент `VerticalUseCaseList`.

## Требования

- [uv](https://docs.astral.sh/uv/) (Astral)
- Python 3.14+
- Node.js и npm — для сборки `components/vertical_use_case_list`

## Установка

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh   # если uv ещё нет

cd components/vertical_use_case_list/frontend
npm install
cd ..
uv run gradio cc build
cd ../..
uv sync
```

## Запуск

```bash
uv run run-app
```

Открой URL из терминала (обычно `http://127.0.0.1:7860`).

Рабочий каталог процесса = место, где лежит `configuration.json`.

## Интерфейс

### Вкладки Kafka / ClickHouse

Слева — список use case: выбор, переименование, drag-and-drop, добавление. Кнопка 🗑 появляется только при включённом **Разрешить удаление**.

Справа — форма сценария и **Send**; логи — в accordion «Console logs».

### Строка статуса

Между шпаргалкой (подсказки по директивам) и вкладками — на всю ширину:

- текст состояния сохранения;
- кнопка **Конфигурация** — открывает боковую панель.

### Панель «Конфигурация»

По умолчанию свёрнута. Открыть: кнопка **Конфигурация** или ручка на левом краю экрана.

Внутри:

| Элемент | Действие |
|---------|----------|
| **Разрешить удаление** | показывает 🗑 в списке use case; без галочки удаление блокируется и на сервере |
| **Сохранить** | запись `configuration.json` |
| **Загрузить** | чтение `configuration.json` с диска (несохранённые правки в форме теряются) |
| **Бэкап** | снимок в `configuration.backups/`; таймер автобэкапа сбрасывается на 30 мин |

Значение галочки при старте — в `src/console/settings.py`:

```python
ALLOW_USE_CASE_DELETE = False
```

## Сохранение

| Событие | Когда пишется на диск |
|---------|------------------------|
| Sidebar: смена / add / delete use case | сразу |
| **Send** | сразу |
| Правка полей формы | через ~2 с после последнего изменения (autosave) |
| **Сохранить** | сразу |
| **Загрузить** | не пишет; только читает файл |
| F5 / открытие страницы | читает `configuration.json` с диска |

Статусы: `Загружено из configuration.json`, `● Несохранённые изменения`, `Автосохранено HH:MM:SS.mmm`, `Сохранено вручную …`, `Бэкап configuration-….json`, `Ошибка сохранения: …`.

Файлы `configuration.json` и `configuration.backups/` в git не коммитятся.

### Бэкапы

- каталог: `configuration.backups/`;
- имя: `configuration-YYYYMMDD-HHMMSS.json`;
- автоматически: каждые **30 мин** (снимок из памяти, не обязательно совпадает с файлом на диске при `dirty`);
- хранится **10** последних;
- откат: скопировать бэкап → `configuration.json` → **Загрузить** или F5.

## Структура репозитория

```
src/console/
  app.py              # Gradio UI, create_app()
  handlers.py         # обработчики событий (save, sidebar, send, backup)
  persistence.py      # autosave, бэкапы, статусы
  use_case_state.py   # CRUD use case, sidebar state
  settings.py         # ALLOW_USE_CASE_DELETE
  parser.py, runner.py
components/vertical_use_case_list/   # Gradio custom component (Svelte 5)
tests/                             # pytest
```

## Лицензия

MIT — см. [LICENSE](LICENSE).

## История версий

[CHANGELOG.md](CHANGELOG.md)
