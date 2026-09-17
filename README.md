# Iceberg + Spark playground

Учебный стенд для изучения Apache Iceberg поверх Apache Spark: локальный
кластер из S3-совместимого хранилища (MinIO), REST-каталога Iceberg и
Jupyter-ноутбука с PySpark, поднимаемый одной командой `docker compose up`.

## Архитектура

Стенд описан в `docker-compose.yml` и состоит из четырёх сервисов:

| Сервис           | Образ                                    | Порты                | Роль |
|------------------|-------------------------------------------|-----------------------|------|
| `minio`          | `minio/minio`                             | `9000` (S3 API), `9001` (консоль) | S3-совместимое хранилище. Здесь физически лежат data-файлы (parquet) и файлы метаданных (avro/json) всех таблиц. Бакет `warehouse` — корень каталога Iceberg. |
| `rest-catalog`   | `apache/iceberg-rest-fixture`             | `8181`                | Iceberg REST Catalog. Хранит указатель на *текущий* `metadata.json` каждой таблицы и обеспечивает атомарную смену снапшотов (commit). Это тестовый fixture-образ Iceberg (используется для проверки соответствия REST-протоколу), а не production-каталог. |
| `spark_notebook` | сборка из `spark_notebook/Dockerfile` (база `jupyter/pyspark-notebook:spark-3.5.0`) | `8888` (JupyterLab), `4040` (Spark UI) | PySpark 3.5.0 с докаченными jar-файлами Iceberg, Hadoop-AWS и AWS SDK. Читает/пишет файлы в MinIO через `S3AFileSystem` и общается с `rest-catalog` по HTTP. |
| `trino`          | `trinodb/trino`                           | `8080` (Web UI / клиенты) | Второй движок поверх тех же таблиц. Конфигурация — в `trino/etc/` (см. ниже), каталог `iceberg` смотрит на тот же `rest-catalog` и тот же бакет `warehouse`, что и каталог `course` у Spark. |

Ключевая идея Iceberg: движок (Spark, Trino — не важно) никогда не ходит
напрямую в файловую систему или в Hive Metastore, чтобы понять "что такое
таблица сейчас". Он спрашивает у каталога (здесь — REST-каталог) путь к
актуальному `metadata.json`, а дальше уже сам читает manifest list →
manifest files → data files из S3. Именно поэтому Spark и Trino в этом
стенде видят одни и те же таблицы и могут писать в них по очереди — таблица
принадлежит каталогу и S3, а не конкретному движку.

```
Jupyter (PySpark) ── SQL/DataFrame API ──▶ rest-catalog (метаданные таблиц) ◀── SQL ── Trino
        │                                          │                                    │
        └──────────── S3AFileSystem ───────────────┴──▶ MinIO (data + metadata файлы) ◀──┘
```

Каталог Iceberg внутри Spark называется `course` и настраивается набором
опций `spark.sql.catalog.course.*`:

* через `spark_notebook/spark-defaults.conf` (единая конфигурация для всех
  ноутбуков, применяется на уровне образа) — см. `spark_notebook/spark-defaults copy.conf`
  за примером полного набора опций (extensions + REST-каталог + S3);
* либо прямо в коде ноутбука через `SparkSession.builder.config(...)` — так
  сделано во всех ноутбуках `app/0*.ipynb`, что делает их самодостаточными и
  не зависящими от текущего содержимого `spark-defaults.conf`.

## Запуск

Стенд поднимается локально через Docker Compose.

### Docker Compose

```bash
docker compose up -d --build
```

После старта:

* JupyterLab — http://localhost:8888 (без токена, см. `command:
  start-notebook.py --IdentityProvider.token=''` в `docker-compose.yml`);
* Консоль MinIO — http://localhost:9001 (логин/пароль: `minioadmin` / `minioadmin`);
* Spark UI — http://localhost:4040 (доступен, пока выполняется job);
* Iceberg REST Catalog — http://localhost:8181/v1/config;
* Trino Web UI — http://localhost:8080.

При первом запуске бакет `warehouse` в MinIO нужно создать один раз вручную
(ни один из сервисов не создаёт его автоматически):

```bash
docker exec minio mc alias set local http://localhost:9000 minioadmin minioadmin
docker exec minio mc mb local/warehouse
```

Рабочая директория `app/` на хосте примонтирована в контейнер `spark_notebook`
как `/app` — все ноутбуки лежат и редактируются прямо там.

Остановить и удалить контейнеры:

```bash
docker compose down
```

## Ноутбуки (`app/`)

Каждый ноутбук самостоятелен: сам поднимает `SparkSession` с нужными
настройками каталога и не зависит от порядка запуска других ноутбуков (кроме
разделов, которые явно опираются на ранее созданные таблицы).

### Базовые примеры (созданы как учебный курс по Iceberg)

| Ноутбук | Что показывает |
|---|---|
| `00_Setup_and_Basics.ipynb` | Из чего состоит стенд, настройка `SparkSession`, создание namespace и таблицы, `INSERT` через SQL и `DataFrameWriterV2.writeTo`, `UPDATE`/`DELETE`, `DESCRIBE TABLE` / `SHOW CREATE TABLE` |
| `01_Schema_Evolution.ipynb` | Безопасная эволюция схемы: `ADD/RENAME/DROP COLUMN`, расширение типа (`INT → BIGINT`), изменение порядка колонок — и почему это не создаёт новых снапшотов данных |
| `02_Partitioning.ipynb` | Hidden partitioning (`days(...)`, `bucket(N, ...)`), partition pruning без явных фильтров по transform-у, partition evolution (`ADD/DROP PARTITION FIELD`) без переписывания старых данных |
| `03_Merge_Into_Upsert.ipynb` | `MERGE INTO` для атомарного применения пакета CDC-изменений (insert/update/delete) одной операцией |
| `04_Table_Maintenance.ipynb` | Обслуживание таблиц: `rewrite_data_files` (компакция мелких файлов), `expire_snapshots` (очистка старых снапшотов), `remove_orphan_files` (удаление файлов-сирот), `rewrite_manifests` (консолидация манифестов) |
| `Table_structure.ipynb` | Внутреннее устройство файлов метаданных — читает "сырые" manifest file, manifest list и `metadata.json` напрямую из S3 через `spark.read.format("avro"/"json")`, в обход каталога |
| `History.ipynb` | Просмотр истории изменений таблицы через служебные (metadata) таблицы `.history`, `.metadata_log_entries`, `.manifests`, `.all_manifests` |
| `Time_travel.ipynb` | Путешествия во времени: `VERSION AS OF <snapshot_id>`, `TIMESTAMP AS OF '<ts>'` |
| `Time_travel_procedure.ipynb` | Управление снапшотами через `CALL course.system.*`: `rollback_to_snapshot`, `rollback_to_timestamp`, `set_current_snapshot` |
| `CDC.ipynb` | Change Data Capture: сравнение двух снапшотов вручную (`VERSION AS OF` + `FULL OUTER JOIN`), встроенная `.changes`-таблица, `CALL course.system.create_changelog_view` |

## Trino (`trino/`)

Второй SQL-движок поверх того же REST Catalog и того же бакета MinIO — чтобы
показать, что Iceberg-таблица не привязана к конкретному движку.
Конфигурация Trino:

```
trino/
  etc/
    node.properties           # id узла, окружение
    config.properties         # single-node coordinator, порт 8080
    jvm.config                # параметры JVM
    log.properties            # уровень логирования
    catalog/iceberg.properties  # каталог "iceberg": REST Catalog + S3 (MinIO)
  examples/                   # SQL-примеры, см. таблицу ниже; смонтированы
                               # в контейнер trino как /examples
```

Каталог `iceberg` в Trino настроен на тот же `rest-catalog:8181` и тот же
`s3://warehouse`, что и каталог `course` в Spark (`trino/etc/catalog/iceberg.properties`).
Соответствие имён: `course.<ns>.<table>` в Spark — это `iceberg.<ns>.<table>`
в Trino, одна и та же таблица.

Запуск примера через `trino` CLI внутри контейнера:

```bash
docker exec -it trino trino                       # интерактивный CLI
docker exec -i trino trino -f /examples/01_basics.sql   # выполнить файл целиком
```

| Файл | Что показывает |
|---|---|
| `trino/examples/01_basics.sql` | `CREATE SCHEMA`/`CREATE TABLE`, `INSERT`, `SELECT`, `UPDATE`/`DELETE` (merge-on-read через delete-файлы), `SHOW CREATE TABLE`, `ALTER TABLE ADD COLUMN` |
| `trino/examples/02_cross_engine_with_spark.sql` | Один и тот же namespace/таблица читается и пишется поочерёдно из Trino и из Spark (`app/00_Setup_and_Basics.ipynb`); разобран нюанс совместимости с векторизованным ридером Parquet в Spark |
| `trino/examples/03_time_travel_and_metadata.sql` | Служебные (metadata) таблицы `$history`, `$snapshots`, `$files`, `$manifests`, `$partitions`; `FOR VERSION AS OF` / `FOR TIMESTAMP AS OF` |
| `trino/examples/04_maintenance.sql` | `ALTER TABLE ... EXECUTE optimize` (компакция файлов), `expire_snapshots`, `remove_orphan_files` |

### Trino и metadata-таблицы: тот случай, когда Trino проще, чем Spark

В разделе ниже описана проблема, из-за которой в Spark на этом стенде не
работает `SELECT * FROM course.test.basics.history` — REST-каталог
(`apache/iceberg-rest-fixture`) отклоняет промежуточный запрос, который
Spark-клиент делает, пробуя понять, что `history` — это служебная таблица, а
не часть namespace. В Trino этой проблемы нет: служебные таблицы адресуются
суффиксом `$` в имени самой таблицы (`iceberg.test."basics$history"`), и
Trino разбирает этот суффикс сам, на своей стороне, ещё до похода в
REST-каталог — поэтому проблемного запроса к каталогу просто не возникает.
Все примеры в `03_time_travel_and_metadata.sql` были проверены на этом стенде
и работают как есть.

## Известная особенность стенда: metadata-таблицы через REST-каталог

У любой Iceberg-таблицы "бесплатно" есть служебные metadata-таблицы —
`history`, `snapshots`, `files`, `partitions`, `manifests` и т.д., обычно
доступные простым SQL `SELECT * FROM catalog.ns.table.history`.

**В этом стенде такой синтаксис не работает через `rest-catalog`** и падает с
ошибкой вида `BadRequestException: Suspicious Path Character`. Причина: перед
тем как правильно понять, что `history` — это имя служебной таблицы, а не
часть namespace, клиент Iceberg сначала пробует (и в норме молча
откатывается) трактовать `test.basics.history` как namespace `test.basics` +
таблицу `history`. Такой составной namespace REST-протокол кодирует в URL
служебным управляющим символом, а образ `apache/iceberg-rest-fixture`
использует Jetty со строгой проверкой пути, который отклоняет такой URL
кодом `400` раньше, чем клиент успевает откатиться к правильной
интерпретации. Это ограничение конкретно тестового REST-каталога, а не
Iceberg как формата — и оно же ломает соответствующие запросы в старых
ноутбуках (`History.ipynb`, `CDC.ipynb` и т.д.), если их запускать в этом
окружении.

Обходной путь, использованный в `00_Setup_and_Basics.ipynb` –
`04_Table_Maintenance.ipynb`, — функция `read_metadata_table(spark, table,
metadata_type)`, которая читает те же данные через Java API Iceberg
(`Spark3Util.loadIcebergTable` + `SparkTableUtil.loadMetadataTable`), вообще
не делая второй сетевой запрос к каталогу:

```python
def read_metadata_table(spark, table_identifier, metadata_type):
    jvm = spark._jvm
    table = jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(spark._jsparkSession, table_identifier)
    mtype = jvm.org.apache.iceberg.MetadataTableType.valueOf(metadata_type.upper())
    jdf = jvm.org.apache.iceberg.spark.SparkTableUtil.loadMetadataTable(spark._jsparkSession, table, mtype)
    return DataFrame(jdf, spark)

read_metadata_table(spark, "course.test.basics", "history").show(truncate=False)
```

`CALL course.system.*` процедуры и `VERSION AS OF` / `TIMESTAMP AS OF` этой
проблемы не имеют и работают как обычно — их идентификаторы всегда
одноуровневые.

## Структура репозитория

```
docker-compose.yml            # оркестрация minio + rest-catalog + spark_notebook + trino
.dockerignore                 # исключения для сборки образа (build context — корень репозитория)
spark_notebook/
  Dockerfile                  # jupyter/pyspark-notebook + jar'ы Iceberg/Hadoop-AWS/AWS SDK
  spark-defaults.conf         # активная конфигурация Spark в образе (сейчас без catalog.course.*)
  spark-defaults copy.conf    # конфигурация с полным набором catalog.course.* (REST + S3)
app/                          # ноутбуки (см. таблицу выше), монтируются в контейнер как /app
trino/
  etc/                        # конфигурация Trino (catalog/iceberg.properties + node/config/jvm/log)
  examples/                   # SQL-примеры Trino + Iceberg (см. раздел "Trino" выше)
minio_data/                   # данные MinIO (bucket warehouse и т.д.), персистентны между запусками
```

## Полезные ссылки

* [Apache Iceberg — документация](https://iceberg.apache.org/docs/latest/)
* [Iceberg Spark DDL / DML / процедуры](https://iceberg.apache.org/docs/latest/spark-ddl/)
* [Iceberg REST Catalog Open API spec](https://github.com/apache/iceberg/tree/main/open-api)
* [Trino Iceberg connector — документация](https://trino.io/docs/current/connector/iceberg.html)
