CREATE VIEW gold.v_station_current AS
SELECT
  station_key, station_id, name, physical_configuration,
  lat, lon, altitude, address,
  district, neighbourhood, post_code,
  capacity, is_charging_station, short_name
FROM gold.dim_station_history
WHERE is_current = 1;

GO