📋 ТЕХНИЧЕСКИЕ ТРЕБОВАНИЯ (SPEC)

## 1. Стек и код-стиль

- Язык: Python 3.13+.
- UI: Gradio (нативные компоненты).
- Интеграции: `aiokafka`, `clickhouse-connect`.
- Стиль: минимализм, строгая типизация, `@dataclass(slots=True)`.
- Не использовать deprecated-компоненты.

## 2. Интерфейс

- Две горизонтальные вкладки:
  - `Kafka`
  - `ClickHouse`
- В каждой вкладке:
  - вертикальный список `Use case`;
  - выбор use case кликом;
  - действия: `+`, переименование, удаление (последний удалять нельзя).
- Общая справка сверху:
  - общая директива `@timeout`;
  - hooks как Python script;
  - ClickHouse-специфика: `@clickhouse.table`.

## 3. Поля форм

### Kafka

- `Bootstrap URL` (`host:port`)
- `Topic`
- `Hooks` (Python script)
- `Messages`
- `Global interval (ms)`
- `Send`
- `Status`

### ClickHouse

- `Endpoint` (`host:port`)
- `Hooks` (Python script)
- `Messages`
- `Global interval (ms)`
- `Send`
- `Status`

## 4. Директивы и парсинг

### Общая директива

- `@timeout <value>` или `@timeout=<value>`
- Поддержка единиц: `ms`, `s`, а также число без суффикса (ms).
- Одноразовая логика:
  - директива влияет только на следующее сообщение;
  - потом снова используется `Global interval (ms)`.

### ClickHouse-специфичная директива

- `@clickhouse.table <table_name>` или `@clickhouse.table=<table_name>`
- Директива задает таблицу для последующих JSON-строк.
- Для JSON без активной `@clickhouse.table` — ошибка.

### Парсинг сообщений

- Парсинг построчный.
- Пустые строки игнорируются.
- Якоря `{{name}}` подставляются из `fields`.

## 5. Hooks

- Hooks выполняются как обычный Python script (trusted mode).
- Допускаются `import`, `datetime`, арифметика, сторонние библиотеки из окружения.
- Контракт:
  - script обязан заполнять `fields` как `dict`;
  - ключи `fields` должны быть строками.
- Пример:
  - `from uuid import uuid4`
  - `fields["run_id"] = str(uuid4())`

## 6. Отправка

### Kafka

- Каждая строка-сообщение отправляется как bytes.
- Задержки между отправками берутся из парсинга `@timeout`.

### ClickHouse

- Автоматический режим без переключателя:
  - строка, которая парсится как JSON object -> `insert`;
  - любая другая строка -> SQL `command`.
- Задержки между отправками берутся из парсинга `@timeout`.

## 7. Конфигурация

- Файл: `configuration.json`.
- Содержит все use case для Kafka и ClickHouse.
- При старте приложения — автозагрузка из файла.
- Кнопки:
  - `Save to File` — запись текущего состояния;
  - `Load from File` — перезагрузка состояния из файла.

