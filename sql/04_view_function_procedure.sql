-- 04_view_function_procedure.sql
-- View, inline table-valued function and stored procedure on top of the star schema.
-- In the Fabric SQL editor, select and run each CREATE statement on its own.

-- View: current stations only. Stores the is_current = 1 filter once
-- and hides the technical columns (valid_from, valid_to, version_no).
CREATE VIEW gold.v_station_current AS
SELECT
  station_key, station_id, name, physical_configuration,
  lat, lon, altitude, address,
  district, neighbourhood, post_code,
  capacity, is_charging_station, short_name
FROM gold.dim_station_history
WHERE is_current = 1;

-- Check: 554 rows.
SELECT COUNT(*) AS rows_in_view FROM gold.v_station_current;

-- Function: state of every station at a given instant (a view with a parameter).
CREATE FUNCTION gold.fn_station_at (@ts datetime2(0))
RETURNS TABLE
AS RETURN
(
  SELECT station_key, station_id, name, district, capacity, valid_from, valid_to
  FROM gold.dim_station_history
  WHERE @ts >= valid_from AND @ts < valid_to
);

-- Check: station 2 had capacity 0 on 2025-01-20 and 29 on 2025-02-01.
SELECT '2025-01-20 12:00' AS moment, station_id, capacity
FROM gold.fn_station_at('2025-01-20 12:00:00') WHERE station_id = 2
UNION ALL
SELECT '2025-02-01 12:00', station_id, capacity
FROM gold.fn_station_at('2025-02-01 12:00:00') WHERE station_id = 2;

-- Procedure: atomic reload of the dimension and the fact in one transaction.
-- Needed because station_key is recomputed with ROW_NUMBER(): if the dimension
-- changed its keys and the fact did not, the model would be broken.
-- Either both tables are reloaded or neither is touched.
CREATE PROCEDURE gold.usp_reload_star
AS
BEGIN
  BEGIN TRY
    BEGIN TRANSACTION;

    TRUNCATE TABLE gold.fact_station_availability;
    TRUNCATE TABLE gold.dim_station_history;

    INSERT INTO gold.dim_station_history
      (station_key, station_id, version_no, valid_from, valid_to, is_current,
       name, physical_configuration, lat, lon, altitude, address,
       district_code, district, neighbourhood_code, neighbourhood, post_code,
       capacity, is_charging_station, short_name, snapshots)
    SELECT
      ROW_NUMBER() OVER (ORDER BY station_id, version_no),
      station_id, version_no,
      CASE WHEN version_no = 1 THEN CAST('1900-01-01' AS datetime2(0)) ELSE run_start END,
      COALESCE(next_start, CAST('9999-12-31' AS datetime2(0))),
      CASE WHEN next_start IS NULL THEN 1 ELSE 0 END,
      CAST(name AS varchar(100)), CAST(physical_configuration AS varchar(50)),
      lat, lon, altitude, CAST(address AS varchar(100)),
      CAST(district_code AS varchar(10)), CAST(district AS varchar(50)),
      CAST(neighbourhood_code AS varchar(10)), CAST(neighbourhood AS varchar(100)),
      CAST(post_code AS varchar(10)), capacity, is_charging_station,
      CAST(short_name AS varchar(20)), snapshots
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

    INSERT INTO gold.fact_station_availability
      (station_key, station_id, hour_local, readings, avg_bikes_available,
       min_bikes_available, max_bikes_available, avg_docks_available,
       empty_readings, full_readings, out_of_service_readings)
    SELECT
      d.station_key, f.station_id, f.hour_local, f.readings, f.avg_bikes_available,
      f.min_bikes_available, f.max_bikes_available, f.avg_docks_available,
      f.empty_readings, f.full_readings, f.out_of_service_readings
    FROM lh_bicing.gold.fact_station_availability AS f
    JOIN gold.dim_station_history AS d
      ON  d.station_id  = f.station_id
      AND f.hour_local >= d.valid_from
      AND f.hour_local <  d.valid_to;

    COMMIT TRANSACTION;
  END TRY
  BEGIN CATCH
    IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
    THROW;
  END CATCH
END;

-- Run the reload.
EXEC gold.usp_reload_star;

-- Check after reload: 5.014 versions, 4.562.937 fact rows, 0 without key, 554 current.
SELECT
  (SELECT COUNT(*) FROM gold.dim_station_history)                                  AS versions,
  (SELECT COUNT(*) FROM gold.fact_station_availability)                            AS fact_rows,
  (SELECT COUNT(*) FROM gold.fact_station_availability WHERE station_key IS NULL)  AS without_key,
  (SELECT COUNT(*) FROM gold.v_station_current)                                    AS current_stations;