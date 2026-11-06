-- 05_cross_checks_and_data_quality.sql
-- Cross-engine reconciliation and duplicate analysis.

-- Shortcut (created in the Fabric UI, not in SQL):
-- lh_bicing > Tables > dbo > New table shortcut > Microsoft OneLake > wh_bicing >
-- gold.dim_station_history, with passthrough identity. Spark reads it as dbo.dim_station_history.

-- Reconciliation: the capacity computed in Spark (lakehouse fact, from station_capacity)
-- must equal the capacity computed in T-SQL (dimension version).
-- Expected: 4.562.937 rows and 0 different capacities.
SELECT
  COUNT(*) AS fact_rows,
  SUM(CASE WHEN lf.capacity = d.capacity THEN 0 ELSE 1 END) AS different_capacities
FROM lh_bicing.gold.fact_station_availability AS lf
JOIN gold.fact_station_availability AS wf
  ON  wf.station_id = lf.station_id
  AND wf.hour_local = lf.hour_local
JOIN gold.dim_station_history AS d
  ON  d.station_key = wf.station_key;

-- Duplicates in the staging table: none inside October or between September and October.
SELECT
  COUNT(*) AS rows_total,
  COUNT(DISTINCT CONCAT(station_id, '|', last_updated)) AS distinct_by_key,
  (SELECT COUNT(*) FROM (SELECT DISTINCT * FROM stg.station_info) AS d) AS distinct_full_row
FROM stg.station_info;

-- Duplicates in bronze (12 months): 4.156 repeated keys, 8.312 rows,
-- between 2024-11-30 and 2025-06-30 (the months whose files carry the extra V1 column).
SELECT
  COUNT(*)          AS repeated_keys,
  SUM(n)            AS rows_involved,
  MIN(last_updated) AS first_mark,
  MAX(last_updated) AS last_mark
FROM (
  SELECT station_id, last_updated, COUNT(*) AS n
  FROM lh_bicing.bronze.station_info
  GROUP BY station_id, last_updated
  HAVING COUNT(*) > 1
) AS d;

-- Deduplication: ROW_NUMBER by business key removes exactly the 4.156 repeats.
-- DISTINCT * removes none, because the repeated rows differ in source_file.
-- Expected: 4.583.559 / 4.579.403 / 4.583.559.
SELECT
  (SELECT COUNT(*) FROM lh_bicing.bronze.station_info) AS rows_total,
  (SELECT COUNT(*) FROM (
      SELECT ROW_NUMBER() OVER (PARTITION BY station_id, last_updated ORDER BY station_id) AS rn
      FROM lh_bicing.bronze.station_info) AS x
   WHERE rn = 1)                                          AS after_row_number,
  (SELECT COUNT(*) FROM (SELECT DISTINCT * FROM lh_bicing.bronze.station_info) AS d) AS after_distinct;

-- DISTINCT without the lineage columns (source_file, loaded_at) still leaves 534 extra rows
-- (4.579.937 instead of 4.579.403): at the June/July 2025 file boundary, V1 has a value in one
-- copy and NULL in the other, because July files do not carry that column.
-- This query finds those 534 keys.
SELECT
  COUNT(*)       AS keys_with_v1_in_one_row_only,
  MIN(file_a)    AS first_file,
  MAX(file_b)    AS last_file
FROM (
  SELECT station_id, last_updated,
         MIN(source_file) AS file_a,
         MAX(source_file) AS file_b
  FROM lh_bicing.bronze.station_info
  GROUP BY station_id, last_updated
  HAVING COUNT(*) > 1
     AND COUNT(V1) > 0
     AND COUNT(V1) < COUNT(*)
) AS d;