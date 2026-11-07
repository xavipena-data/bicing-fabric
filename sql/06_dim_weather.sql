-- 06_dim_weather.sql
-- Hourly weather dimension (Meteocat XEMA, station X4 Barcelona - el Raval)
-- Source: lh_bicing.gold.dim_weather (built in nb_07_weather_silver)

CREATE TABLE gold.dim_weather AS
SELECT
    CAST(hour_local       AS datetime2(6))  AS hour_local,
    CAST(temperature_c    AS decimal(5,2))  AS temperature_c,
    CAST(precipitation_mm AS decimal(6,2))  AS precipitation_mm,
    CAST(wind_ms          AS decimal(5,2))  AS wind_ms,
    CAST(n_readings       AS smallint)      AS n_readings
FROM lh_bicing.gold.dim_weather;

-- Checks
SELECT COUNT(*) AS rows_total,
       SUM(CASE WHEN n_readings = 0 THEN 1 ELSE 0 END) AS hours_without_data
FROM gold.dim_weather;

SELECT COUNT(*) AS hours_without_match
FROM gold.dim_time t
LEFT JOIN gold.dim_weather w ON w.hour_local = t.hour_local
WHERE w.hour_local IS NULL;

SELECT MIN(temperature_c) AS min_t, MAX(temperature_c) AS max_t,
       MAX(precipitation_mm) AS max_rain, MAX(wind_ms) AS max_wind
FROM gold.dim_weather;