CREATE FUNCTION gold.fn_station_at (@ts datetime2(0))
RETURNS TABLE
AS RETURN
(
  SELECT station_key, station_id, name, district, capacity, valid_from, valid_to
  FROM gold.dim_station_history
  WHERE @ts >= valid_from AND @ts < valid_to
);

GO