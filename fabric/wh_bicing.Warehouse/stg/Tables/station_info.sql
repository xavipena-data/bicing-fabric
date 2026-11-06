CREATE TABLE [stg].[station_info] (
    [station_id]             VARCHAR (20)  NULL,
    [name]                   VARCHAR (200) NULL,
    [physical_configuration] VARCHAR (50)  NULL,
    [lat]                    VARCHAR (30)  NULL,
    [lon]                    VARCHAR (30)  NULL,
    [altitude]               VARCHAR (30)  NULL,
    [address]                VARCHAR (200) NULL,
    [cross_street]           VARCHAR (200) NULL,
    [post_code]              VARCHAR (20)  NULL,
    [capacity]               VARCHAR (20)  NULL,
    [is_charging_station]    VARCHAR (20)  NULL,
    [short_name]             VARCHAR (30)  NULL,
    [nearby_distance]        VARCHAR (20)  NULL,
    [x_ride_code_support]    VARCHAR (20)  NULL,
    [rental_uris]            VARCHAR (500) NULL,
    [is_valet_station]       VARCHAR (20)  NULL,
    [last_updated]           VARCHAR (20)  NULL,
    [ttl]                    VARCHAR (20)  NULL
);


GO