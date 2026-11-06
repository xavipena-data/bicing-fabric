CREATE TABLE [gold].[dim_time] (
    [hour_local]   DATETIME2 (6)  NULL,
    [date]         DATE           NULL,
    [year]         INT            NULL,
    [month]        INT            NULL,
    [month_name]   VARCHAR (8000) NULL,
    [year_month]   VARCHAR (8000) NULL,
    [day_of_month] INT            NULL,
    [day_of_week]  INT            NULL,
    [day_name]     VARCHAR (8000) NULL,
    [is_weekend]   BIT            NULL,
    [hour_of_day]  INT            NULL,
    [day_part]     VARCHAR (8000) NULL,
    [season]       VARCHAR (8000) NULL
);


GO