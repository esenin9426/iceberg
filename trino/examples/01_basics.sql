-- ============================================================================
-- 01. Trino + Iceberg: базовые операции
-- ============================================================================
-- Запуск целиком:
--   docker exec -i trino trino --file /examples/01_basics.sql
-- Или построчно в интерактивном CLI:
--   docker exec -it trino trino
--
-- Каталог "iceberg" в Trino (trino/etc/catalog/iceberg.properties) смотрит
-- на тот же REST Catalog (rest-catalog:8181) и тот же бакет MinIO
-- (s3://warehouse), что и каталог "course" в Spark. Поэтому обращение к
-- таблице выглядит как <catalog>.<namespace>.<table>, например
-- iceberg.test.basics — прямой аналог course.test.basics из ноутбуков.
-- ============================================================================

-- Namespace в Iceberg = schema в терминах Trino.
CREATE SCHEMA IF NOT EXISTS iceberg.test
WITH (location = 's3://warehouse/test');

SHOW SCHEMAS FROM iceberg;

-- Создание таблицы: по умолчанию iceberg.format-version = 2 (copy-on-write
-- для insert/append, merge-on-read для update/delete).
CREATE TABLE IF NOT EXISTS iceberg.test.basics (
    id   BIGINT,
    name VARCHAR,
    city VARCHAR
);

SHOW CREATE TABLE iceberg.test.basics;

-- INSERT создаёт новый снапшот таблицы (append).
INSERT INTO iceberg.test.basics VALUES
    (1, 'Alice', 'Moscow'),
    (2, 'Bob',   'Berlin');

SELECT * FROM iceberg.test.basics ORDER BY id;

-- UPDATE/DELETE в Iceberg v2 реализованы через delete-файлы (merge-on-read):
-- строки физически не переписываются, вместо этого пишется delete-файл,
-- который "гасит" старые записи при чтении.
UPDATE iceberg.test.basics SET city = 'Paris' WHERE id = 2;

DELETE FROM iceberg.test.basics WHERE id = 1;

SELECT * FROM iceberg.test.basics ORDER BY id;

-- Схему можно эволюционировать так же безопасно, как в Spark:
ALTER TABLE iceberg.test.basics ADD COLUMN signup_date DATE;

DESCRIBE iceberg.test.basics;
