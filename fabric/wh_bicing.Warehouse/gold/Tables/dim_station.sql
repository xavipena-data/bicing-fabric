CREATE TABLE [gold].[dim_station] (
    [station_id]             INT           NULL,
    [name]                   VARCHAR (100) NULL,
    [station_kind]           VARCHAR (20)  NULL,
    [physical_configuration] VARCHAR (50)  NULL,
    [address]                VARCHAR (100) NULL,
    [lat]                    FLOAT (53)    NULL,
    [lon]                    FLOAT (53)    NULL,
    [altitude]               FLOAT (53)    NULL,
    [district_code]          VARCHAR (10)  NULL,
    [district]               VARCHAR (50)  NULL,
    [neighbourhood_code]     VARCHAR (10)  NULL,
    [neighbourhood]          VARCHAR (100) NULL,
    [post_code]              VARCHAR (10)  NULL,
    [is_charging_station]    BIT           NULL,
    [first_seen_at]          DATETIME2 (6) NULL,
    [last_seen_at]           DATETIME2 (6) NULL
);


GO