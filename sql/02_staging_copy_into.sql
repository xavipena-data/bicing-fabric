-- 02_staging_copy_into.sql
-- Loads two monthly station-info CSVs into a staging table with COPY INTO.
-- COPY INTO from OneLake is in public preview at the time of writing.
-- Replace <workspace-id> and <lakehouse-id> with the ids shown in the Fabric URL
-- of the workspace and of lh_bicing.

CREATE SCHEMA stg;

-- Staging keeps everything as text, exactly as it arrives. The source files contain
-- the literal string 'NA' in several columns, so typing happens later with TRY_CAST.
CREATE TABLE stg.station_info (
  station_id             varchar(20),
  name                   varchar(200),
  physical_configuration varchar(50),
  lat                    varchar(30),
  lon                    varchar(30),
  altitude               varchar(30),
  address                varchar(200),
  cross_street           varchar(200),
  post_code              varchar(20),
  capacity               varchar(20),
  is_charging_station    varchar(20),
  short_name             varchar(30),
  nearby_distance        varchar(20),
  x_ride_code_support    varchar(20),
  rental_uris            varchar(500),
  is_valet_station       varchar(20),
  last_updated           varchar(20),
  ttl                    varchar(20)
);

-- FIRSTROW = 2 skips the header. Columns are matched by position.
COPY INTO stg.station_info
FROM 'https://onelake.dfs.fabric.microsoft.com/<workspace-id>/<lakehouse-id>/Files/bronze/bicing/info/info_2025-10.csv'
WITH (FILE_TYPE = 'CSV', FIRSTROW = 2);

COPY INTO stg.station_info
FROM 'https://onelake.dfs.fabric.microsoft.com/<workspace-id>/<lakehouse-id>/Files/bronze/bicing/info/info_2025-09.csv'
WITH (FILE_TYPE = 'CSV', FIRSTROW = 2);

-- Check: October alone has 377.742 rows; with September, 770.246.
SELECT COUNT(*) AS rows_loaded FROM stg.station_info;

-- CAST fails on 'NA' (Msg 245); TRY_CAST returns NULL.
-- October: 13.754 'NA' in altitude, all 377.742 rows in rental_uris.
SELECT
  SUM(CASE WHEN altitude = 'NA' THEN 1 ELSE 0 END)                      AS altitude_na,
  SUM(CASE WHEN TRY_CAST(altitude AS float) IS NULL THEN 1 ELSE 0 END)  AS altitude_null_after_try_cast
FROM stg.station_info;