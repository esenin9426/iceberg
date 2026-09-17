-- ============================================================================
-- 02. Trino + Spark на одной и той же таблице Iceberg
-- ============================================================================
-- Главная идея Iceberg: таблица — это не "собственность" одного движка, а
-- набор файлов (data + metadata) в S3 плюс запись в каталоге. Spark и Trino
-- в этом стенде смотрят в один и тот же REST Catalog (rest-catalog:8181) и
-- один и тот же бакет (s3://warehouse), поэтому оба видят одни и те же
-- таблицы и могут писать в них по очереди.
--
-- Соответствие имён каталогов:
--   Spark:  course.<namespace>.<table>
--   Trino:  iceberg.<namespace>.<table>
-- Namespace/таблица — одни и те же, отличается только локальный алиас
-- каталога, заданный в конфиге каждого движка.
-- ============================================================================

-- Шаг 1. В Jupyter (app/00_Setup_and_Basics.ipynb) выполните ячейки создания
-- namespace course.test и таблицы course.test.basics с парой INSERT — это
-- даст Spark-таблицу, которую сейчас прочитает Trino.

-- Шаг 2. Читаем ту же таблицу из Trino:
SELECT * FROM iceberg.test.basics ORDER BY id;

SELECT * FROM iceberg.test."basics$snapshots" ORDER BY committed_at;

-- Шаг 3. Пишем в неё из Trino — Spark должен сразу увидеть эти строки.
INSERT INTO iceberg.test.basics (id, name, city) VALUES (100, 'Trino Guy', 'Amsterdam');

-- Вернитесь в Jupyter и выполните:
--   spark.sql("SELECT * FROM course.test.basics ORDER BY id").show()
-- Строка (100, 'Trino Guy', 'Amsterdam') будет видна сразу, без перезапуска
-- Spark-сессии — оба движка каждый раз заново спрашивают у REST Catalog
-- актуальный metadata.json.

-- ----------------------------------------------------------------------------
-- Известный нюанс совместимости: векторизованное чтение Parquet в Spark
-- ----------------------------------------------------------------------------
-- Trino по умолчанию пишет строковые колонки Parquet с кодировкой
-- DELTA_LENGTH_BYTE_ARRAY. Векторизованный (Arrow-based) ридер Iceberg,
-- который использует Spark 3.5 + iceberg-spark-runtime 1.10.1 (см.
-- spark_notebook/Dockerfile), эту кодировку не поддерживает и падает с
-- ошибкой вида:
--
--   UnsupportedOperationException: Cannot support vectorized reads for
--   column [<col>] ... with encoding DELTA_LENGTH_BYTE_ARRAY
--
-- Это ограничение конкретно векторизованного пути чтения в этой версии
-- Iceberg/Spark, а не формата как такового: обычный (non-vectorized) ридер
-- читает такие файлы без проблем. Обходной путь — отключить векторизацию
-- на стороне Spark при чтении таблиц, в которые писал Trino:
--
--   spark = (
--       SparkSession.builder
--       ...
--       .config("spark.sql.iceberg.vectorization.enabled", "false")
--       .getOrCreate()
--   )
--
-- Обратное направление (Trino читает таблицы, написанные Spark) работает
-- без каких-либо дополнительных настроек — Trino сам не векторизует чтение
-- по умолчанию для смешанных кодировок.

-- Очистка (опционально):
-- DELETE FROM iceberg.test.basics WHERE id = 100;
