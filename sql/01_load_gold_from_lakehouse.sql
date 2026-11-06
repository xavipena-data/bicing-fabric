-- 01_load_gold_from_lakehouse.sql
-- Copies the gold tables from lh_bicing into wh_bicing with CTAS.
-- Run once, in order, on an empty warehouse.

CREATE SCHEMA gold;

-- Current-version station dimension.
-- Text columns are CAST to the real maximum lengths (measured with MAX(LEN(...)))
-- instead of the varchar(8000) that a plain CTAS from Spark strings produces.
CREATE TABLE gold.dim_station AS
SELECT
  station_id,
  CAST(name AS varchar(100))                   AS name,
  CAST(station_kind AS varchar(20))            AS station_kind,
  CAST(physical_configuration AS varchar(50))  AS physical_configuration,
  CAST(address AS varchar(100))                AS address,
  lat, lon, altitude,
  CAST(district_code AS varchar(10))           AS district_code,
  CAST(district AS varchar(50))                AS district,
  CAST(neighbourhood_code AS varchar(10))      AS neighbourhood_code,
  CAST(neighbourhood AS varchar(100))          AS neighbourhood,
  CAST(post_code AS varchar(10))               AS post_code,
  is_charging_station,
  first_seen_at,
  last_seen_at
FROM lh_bicing.gold.dim_station;

-- Capacity ranges and the hourly calendar, copied as they are.
CREATE TABLE gold.station_capacity AS
SELECT * FROM lh_bicing.gold.station_capacity;

CREATE TABLE gold.dim_time AS
SELECT * FROM lh_bicing.gold.dim_time;

-- Checks: 554 stations, 4.938 capacity ranges, 8.760 hours.
SELECT 'dim_station' AS tabla, COUNT(*) AS filas FROM gold.dim_station
UNION ALL SELECT 'station_capacity', COUNT(*) FROM gold.station_capacity
UNION ALL SELECT 'dim_time', COUNT(*) FROM gold.dim_time;