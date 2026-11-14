-- 07_exclude_stations.sql
-- Warehouse wh_bicing. Run after nb_11_exclude_stations.
--
-- Applies to the warehouse the same station exclusion that nb_11 applies to the lakehouse
-- (stations 536, 542, 543, 545, 588 and 597), so that both copies of the gold model
-- hold the same 548 stations.
-- Running it twice is harmless: the second run deletes nothing.

-- 1. Before: how many rows will go.
--    Expected: fact 4.562.937 rows (2.672 to remove), dim_station 554 (6),
--    dim_station_history 5.014 (8).
SELECT 'fact_station_availability' AS tabla, COUNT(*) AS filas_total,
       SUM(CASE WHEN station_id IN (536,542,543,545,588,597) THEN 1 ELSE 0 END) AS filas_a_quitar
FROM gold.fact_station_availability
UNION ALL
SELECT 'dim_station', COUNT(*),
       SUM(CASE WHEN station_id IN (536,542,543,545,588,597) THEN 1 ELSE 0 END)
FROM gold.dim_station
UNION ALL
SELECT 'dim_station_history', COUNT(*),
       SUM(CASE WHEN station_id IN (536,542,543,545,588,597) THEN 1 ELSE 0 END)
FROM gold.dim_station_history;

-- 2. Delete, in one transaction: either all three tables change or none.
BEGIN TRANSACTION;

DELETE FROM gold.fact_station_availability WHERE station_id IN (536,542,543,545,588,597);
DELETE FROM gold.dim_station               WHERE station_id IN (536,542,543,545,588,597);
DELETE FROM gold.dim_station_history       WHERE station_id IN (536,542,543,545,588,597);

COMMIT TRANSACTION;

-- 3. After: run the query of step 1 again.
--    Expected: fact 4.560.265 rows, dim_station 548, dim_station_history 5.006,
--    and filas_a_quitar = 0 in the three rows.