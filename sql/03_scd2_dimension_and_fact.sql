-- 03_scd2_dimension_and_fact.sql
-- Builds the SCD type 2 station dimension from silver.station_info and the hourly
-- availability fact pointing at the dimension version valid in each hour.

-- silver.station_info keeps one row per station version, with UTC first/last seen.
-- valid_from  : first_seen_at converted to Madrid local time and truncated to the hour;
--               1900-01-01 for version 1, so the fact never falls before the first version.
-- valid_to    : valid_from of the next version, or 9999-12-31 for the current one.
-- station_key : surrogate key, one number per version.
CREATE TABLE gold.dim_station_history AS
SELECT
  ROW_NUMBER() OVER (ORDER BY station_id, version_no)          AS station_key,
  station_id,
  version_no,
  CASE WHEN version_no = 1
       THEN CAST('1900-01-01' AS datetime2(0))
       ELSE run_start END                                       AS valid_from,
  COALESCE(next_start, CAST('9999-12-31' AS datetime2(0)))      AS valid_to,
  CASE WHEN next_start IS NULL THEN 1 ELSE 0 END                AS is_current,
  CAST(name AS varchar(100))                   AS name,
  CAST(physical_configuration AS varchar(50))  AS physical_configuration,
  lat, lon, altitude,
  CAST(address AS varchar(100))                AS address,
  CAST(district_code AS varchar(10))           AS district_code,
  CAST(district AS varchar(50))                AS district,
  CAST(neighbourhood_code AS varchar(10))      AS neighbourhood_code,
  CAST(neighbourhood AS varchar(100))          AS neighbourhood,
  CAST(post_code AS varchar(10))               AS post_code,
  capacity,
  is_charging_station,
  CAST(short_name AS varchar(20))              AS short_name,
  snapshots
FROM (
  SELECT t.*,
         LEAD(run_start) OVER (PARTITION BY station_id ORDER BY version_no) AS next_start
  FROM (
    SELECT s.*,
           DATETRUNC(hour, CAST(s.first_seen_at AT TIME ZONE 'UTC'
                                AT TIME ZONE 'Romance Standard Time' AS datetime2(0))) AS run_start
    FROM lh_bicing.silver.station_info AS s
  ) AS t
) AS x;

-- Check: 5.014 versions, 554 stations, 554 current, 0 empty ranges.
SELECT
  COUNT(*)                                                AS versions,
  COUNT(DISTINCT station_id)                              AS stations,
  SUM(is_current)                                         AS current_versions,
  SUM(CASE WHEN valid_from >= valid_to THEN 1 ELSE 0 END) AS empty_ranges
FROM gold.dim_station_history;

-- Every fact row must match exactly one dimension version.
-- The three numbers must be equal: 4.562.937.
SELECT
  COUNT(*) AS rows_after_join,
  COUNT(DISTINCT CONCAT(f.station_id, '|', CAST(f.hour_local AS varchar(30)))) AS distinct_rows,
  (SELECT COUNT(*) FROM lh_bicing.gold.fact_station_availability) AS fact_rows
FROM lh_bicing.gold.fact_station_availability AS f
JOIN gold.dim_station_history AS d
  ON  d.station_id  = f.station_id
  AND f.hour_local >= d.valid_from
  AND f.hour_local <  d.valid_to;

-- Fact table: keys and measures only. Capacity lives in the dimension, versioned.
CREATE TABLE gold.fact_station_availability AS
SELECT
  d.station_key,
  f.station_id,
  f.hour_local,
  f.readings,
  f.avg_bikes_available,
  f.min_bikes_available,
  f.max_bikes_available,
  f.avg_docks_available,
  f.empty_readings,
  f.full_readings,
  f.out_of_service_readings
FROM lh_bicing.gold.fact_station_availability AS f
JOIN gold.dim_station_history AS d
  ON  d.station_id  = f.station_id
  AND f.hour_local >= d.valid_from
  AND f.hour_local <  d.valid_to;

-- Check: 4.562.937 rows, all with a station_key.
SELECT COUNT(*) AS fact_rows, COUNT(station_key) AS with_key
FROM gold.fact_station_availability;

-- Example use of the star schema: average occupancy by district,
-- with the capacity valid in each hour.
SELECT TOP 5
  d.district,
  CAST(SUM(f.avg_bikes_available) / NULLIF(SUM(d.capacity), 0) AS decimal(5,3)) AS occupancy
FROM gold.fact_station_availability AS f
JOIN gold.dim_station_history AS d ON d.station_key = f.station_key
GROUP BY d.district
ORDER BY occupancy DESC;