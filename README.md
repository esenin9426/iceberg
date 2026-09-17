# Iceberg + Spark playground

Учебный стенд для изучения Apache Iceberg поверх Apache Spark: локальный
кластер из S3-совместимого хранилища (MinIO), REST-каталога Iceberg и
Jupyter-ноутбука с PySpark, поднимаемый одной командой `docker compose up`.

## Архитектура

Стенд описан в `docker-compose.yml` и состоит из трёх сервисов:

| Сервис           | Образ                                    | Порты                | Роль |
|------------------|-------------------------------------------|-----------------------|------|
| `minio`          | `minio/minio`                             | `9000` (S3 API), `9001` (консоль) | S3-совместимое хранилище. Здесь физически лежат data-файлы (parquet) и файлы метаданных (avro/json) всех таблиц. Бакет `warehouse` — корень каталога Iceberg. |
| `rest-catalog`   | `apache/iceberg-rest-fixture`             | `8181`                | Iceberg REST Catalog. Хранит указатель на *текущий* `metadata.json` каждой таблицы и обеспечивает атомарную смену снапшотов (commit). Это тестовый fixture-образ Iceberg (используется для проверки соответствия REST-протоколу), а не production-каталог. |
| `spark_notebook` | сборка из `spark_notebook/Dockerfile` (база `jupyter/pyspark-notebook:spark-3.5.0`) | `8888` (JupyterLab), `4040` (Spark UI) | PySpark 3.5.0 с докаченными jar-файлами Iceberg, Hadoop-AWS и AWS SDK. Читает/пишет файлы в MinIO через `S3AFileSystem` и общается с `rest-catalog` по HTTP. |

Ключевая идея Iceberg: Spark никогда не ходит напрямую в файловую систему или
в Hive Metastore, чтобы понять "что такое таблица сейчас". Он спрашивает у
каталога (здесь — REST-каталог) путь к актуальному `metadata.json`, а дальше
уже сам читает manifest list → manifest files → data files из S3.

```
Jupyter (PySpark) ── SQL/DataFrame API ──▶ rest-catalog (метаданные таблиц)
        │                                          │
        └──────────── S3AFileSystem ───────────────┴──▶ MinIO (data + metadata файлы)
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
* Iceberg REST Catalog — http://localhost:8181/v1/config.

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
docker-compose.yml            # оркестрация minio + rest-catalog + spark_notebook
.dockerignore                 # исключения для сборки образа (build context — корень репозитория)
spark_notebook/
  Dockerfile                  # jupyter/pyspark-notebook + jar'ы Iceberg/Hadoop-AWS/AWS SDK
  spark-defaults.conf         # активная конфигурация Spark в образе (сейчас без catalog.course.*)
  spark-defaults copy.conf    # конфигурация с полным набором catalog.course.* (REST + S3)
app/                          # ноутбуки (см. таблицу выше), монтируются в контейнер как /app
minio_data/                   # данные MinIO (bucket warehouse и т.д.), персистентны между запусками
```

## Полезные ссылки

* [Apache Iceberg — документация](https://iceberg.apache.org/docs/latest/)
* [Iceberg Spark DDL / DML / процедуры](https://iceberg.apache.org/docs/latest/spark-ddl/)
* [Iceberg REST Catalog Open API spec](https://github.com/apache/iceberg/tree/main/open-api)
