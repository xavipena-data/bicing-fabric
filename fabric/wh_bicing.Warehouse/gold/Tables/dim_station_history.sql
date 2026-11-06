CREATE TABLE [gold].[dim_station_history] (
    [station_key]            BIGINT        NULL,
    [station_id]             INT           NULL,
    [version_no]             BIGINT        NULL,
    [valid_from]             DATETIME2 (0) NULL,
    [valid_to]               DATETIME2 (0) NULL,
    [is_current]             INT           NOT NULL,
    [name]                   VARCHAR (100) NULL,
    [physical_configuration] VARCHAR (50)  NULL,
    [lat]                    FLOAT (53)    NULL,
    [lon]                    FLOAT (53)    NULL,
    [altitude]               FLOAT (53)    NULL,
    [address]                VARCHAR (100) NULL,
    [district_code]          VARCHAR (10)  NULL,
    [district]               VARCHAR (50)  NULL,
    [neighbourhood_code]     VARCHAR (10)  NULL,
    [neighbourhood]          VARCHAR (100) NULL,
    [post_code]              VARCHAR (10)  NULL,
    [capacity]               INT           NULL,
    [is_charging_station]    BIT           NULL,
    [short_name]             VARCHAR (20)  NULL,
    [snapshots]              BIGINT        NULL
);


GO