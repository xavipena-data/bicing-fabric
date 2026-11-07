CREATE TABLE [gold].[dim_weather] (
    [hour_local]       DATETIME2 (6)  NULL,
    [temperature_c]    DECIMAL (5, 2) NULL,
    [precipitation_mm] DECIMAL (6, 2) NULL,
    [wind_ms]          DECIMAL (5, 2) NULL,
    [n_readings]       SMALLINT       NULL
);


GO