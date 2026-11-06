CREATE TABLE [gold].[fact_station_availability] (
    [station_key]             BIGINT        NULL,
    [station_id]              INT           NULL,
    [hour_local]              DATETIME2 (6) NULL,
    [readings]                BIGINT        NULL,
    [avg_bikes_available]     FLOAT (53)    NULL,
    [min_bikes_available]     INT           NULL,
    [max_bikes_available]     INT           NULL,
    [avg_docks_available]     FLOAT (53)    NULL,
    [empty_readings]          INT           NULL,
    [full_readings]           INT           NULL,
    [out_of_service_readings] INT           NULL
);


GO