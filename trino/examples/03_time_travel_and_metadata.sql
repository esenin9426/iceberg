-- ============================================================================
-- 03. Time travel и служебные (metadata) таблицы в Trino
-- ============================================================================
-- В отличие от Spark-примеров этого стенда (см. раздел README про
-- "Suspicious Path Character"), в Trino служебные таблицы работают без
-- обходных путей: Trino адресует их через суффикс $ в имени
-- (iceberg.<ns>."<table>$history"), и разбирает этот суффикс сам, на своей
-- стороне, ещё до обращения к REST Catalog — поэтому проблемный запрос вида
-- "namespace.table.history" (который ломается в Spark-клиенте на этом
-- тестовом REST-каталоге) здесь просто не возникает.
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS iceberg.test
WITH (location = 's3://warehouse/test');

CREATE TABLE IF NOT EXISTS iceberg.test.tt (id BIGINT, name VARCHAR);

INSERT INTO iceberg.test.tt VALUES (1, 'Alice'), (2, 'Bob');

-- Полная история снапшотов: когда, какой snapshot_id, какая операция
-- (append/overwrite/delete/replace), от какого родителя.
SELECT * FROM iceberg.test."tt$history";

SELECT snapshot_id, parent_id, operation, committed_at
FROM iceberg.test."tt$snapshots"
ORDER BY committed_at;

-- Файлы данных текущего снапшота с их статистикой (min/max по колонкам и т.д.)
SELECT file_path, file_format, record_count, file_size_in_bytes
FROM iceberg.test."tt$files";

-- Манифесты и партиции (для tt партиций нет, но таблица доступна всегда)
SELECT * FROM iceberg.test."tt$manifests";
SELECT * FROM iceberg.test."tt$partitions";

-- ----------------------------------------------------------------------------
-- Time travel
-- ----------------------------------------------------------------------------
-- Возьмём snapshot_id и committed_at сразу после первого INSERT, чтобы потом
-- вернуться к состоянию "только Alice и Bob".
-- Замените :snap_id / :snap_ts значениями из запроса ниже перед выполнением
-- блока time travel (либо выполняйте файл через клиент, поддерживающий
-- подстановку переменных).
SELECT snapshot_id, committed_at
FROM iceberg.test."tt$snapshots"
ORDER BY committed_at DESC LIMIT 1;

INSERT INTO iceberg.test.tt VALUES (3, 'Carol');

-- Текущее состояние — все три строки.
SELECT * FROM iceberg.test.tt ORDER BY id;

-- Путешествие по snapshot_id (подставьте id из запроса выше, до INSERT Carol).
-- SELECT * FROM iceberg.test.tt FOR VERSION AS OF <snapshot_id> ORDER BY id;

-- Путешествие по времени (подставьте committed_at из того же запроса).
-- SELECT * FROM iceberg.test.tt FOR TIMESTAMP AS OF TIMESTAMP '<committed_at>' ORDER BY id;

-- Очистка:
DROP TABLE IF EXISTS iceberg.test.tt;
DROP SCHEMA IF EXISTS iceberg.test;
