-- ============================================================================
-- 04. Обслуживание таблиц из Trino
-- ============================================================================
-- Аналог процедур course.system.* из app/04_Table_Maintenance.ipynb, но
-- через синтаксис ALTER TABLE ... EXECUTE, специфичный для Iceberg-коннектора
-- Trino. Запускать через:
--   docker exec -i trino trino -f /examples/04_maintenance.sql
-- (именно `-f`, а не `--execute`, чтобы SET SESSION ниже подействовал на
-- последующие команды в той же CLI-сессии).
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS iceberg.test
WITH (location = 's3://warehouse/test');

CREATE TABLE IF NOT EXISTS iceberg.test.maint (id BIGINT, name VARCHAR);

-- Нарочно создаём много мелких файлов: один файл на INSERT.
INSERT INTO iceberg.test.maint VALUES (1, 'Alice');
INSERT INTO iceberg.test.maint VALUES (2, 'Bob');
INSERT INTO iceberg.test.maint VALUES (3, 'Carol');

SELECT count(*) AS files_before FROM iceberg.test."maint$files";

-- Компакция мелких файлов в более крупные (аналог rewrite_data_files в Spark).
ALTER TABLE iceberg.test.maint EXECUTE optimize;

SELECT count(*) AS files_after_optimize FROM iceberg.test."maint$files";

-- Очистка старых снапшотов (аналог expire_snapshots в Spark).
-- По умолчанию Trino не даст указать retention_threshold короче 7 дней
-- (защита от случайной потери возможности time travel/rollback в
-- production). Для учебного стенда явно снижаем порог через session-
-- свойство — в реальной эксплуатации так делать не стоит.
SET SESSION iceberg.expire_snapshots_min_retention = '0s';
ALTER TABLE iceberg.test.maint EXECUTE expire_snapshots(retention_threshold => '0s');

SELECT count(*) AS snapshots_after_expire FROM iceberg.test."maint$snapshots";

-- Удаление файлов-сирот: файлов в директории таблицы, на которые больше не
-- ссылается ни один манифест (аналог remove_orphan_files в Spark).
SET SESSION iceberg.remove_orphan_files_min_retention = '0s';
ALTER TABLE iceberg.test.maint EXECUTE remove_orphan_files(retention_threshold => '0s');

-- Очистка:
DROP TABLE IF EXISTS iceberg.test.maint;
DROP SCHEMA IF EXISTS iceberg.test;
