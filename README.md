# Bicing Station Availability on Microsoft Fabric

An end-to-end analytics project built on Microsoft Fabric, seen through the eyes of a Bicing operator: which stations run empty or full, at what times, and where.

> **Status:** work in progress. Lakehouse (medallion), warehouse and Eventhouse layers are built, and the modeling dataset with a baseline is ready. Model training, alerting and reporting layers are planned.

## Goal

Answer one operational question: *where and when does the service fail to offer a bike or a free dock?* The analysis covers daily and weekly patterns and seasonality over almost a full year of data (1 November 2024 to 29 October 2025, the last day available).

## Stack

- Microsoft Fabric: Lakehouse, Warehouse, Eventhouse (KQL), Data Pipelines, Notebooks (Spark), Dataflow Gen2, Power BI
- Medallion architecture (bronze, silver, gold)
- T-SQL, KQL and PySpark
- Data Wrangler and pandas for data preparation
- Git integration for Fabric items

## Data

Station status and station information for Barcelona's Bicing service, published by Open Data BCN (Barcelona City Council):
[Bicing stations status](https://opendata-ajuntament.barcelona.cat/data/en/dataset/estat-estacions-bicing)

Licensed under [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/). The raw files are not versioned in this repository (`data/` is ignored); see [How to reproduce](#how-to-reproduce).

### Weather

Half-hourly readings from the Meteocat automatic weather network (XEMA), published as open data through a Socrata (SODA) API (dataset id `nzvn-apee`). The project uses one station, X4 (Barcelona, el Raval), and three variables: temperature (°C), precipitation (mm) and wind speed at 10 m (m/s), for the same period as the Bicing data.

Source: Meteocat, Departament de Territori, Habitatge i Transició Ecològica (Generalitat de Catalunya). The dataset metadata lists the licence as `METEOCAT`. The raw responses are not versioned in this repository.

The raw JSON is downloaded by the pipeline `pl_weather_bronze` (REST connector, one request per variable, half-hourly base) into `Files/raw/weather/`, and loaded with `nb_06_weather_bronze` and `nb_07_weather_silver`. In total there are 52.916 readings, about 17.639 per variable.

Each reading carries a validation status. Of the 52.916 readings, 69,5 % have no status in the source (`N`), 29,4 % are validated (`V`) and 1,1 % are marked `T`. All of them are kept, because keeping only validated readings would drop about 70 % of the data; the status stays in the column `validation_status` of the silver table.

## Architecture

One lakehouse (`lh_bicing`) holds the medallion layers as schemas. A warehouse (`wh_bicing`) holds the gold model for SQL consumers, and an Eventhouse (`eh_bicing`) holds a copy of the data for KQL analysis.

| Item | Type | Role |
|---|---|---|
| `lh_bicing` | Lakehouse | Raw files, bronze, silver and gold tables (schemas `bronze`, `silver`, `gold`) |
| `pl_bicing_bronze` | Data pipeline | Orchestrates the ingestion of the raw files |
| `nb_01_files_to_bronze` | Notebook | Raw files to bronze tables |
| `nb_02_bronze_to_silver` | Notebook | Cleaning and typing of the status data |
| `nb_03_station_info` | Notebook | Station information: bronze, silver, `dim_station` and capacity ranges |
| `nb_04_silver_to_gold` | Notebook | Hourly availability fact table |
| `df_dim_station` | Dataflow Gen2 | The same `dim_station` built with Power Query, to compare both approaches |
| `wh_bicing` | Warehouse | Gold model in T-SQL: SCD type 2 dimension, fact table, view, function, procedure |
| `nb_05_sql_vs_pyspark` | Notebook | Duplicates, nulls and type conversions compared between SQL and PySpark |
| `pl_weather_bronze` | Data pipeline | Downloads the Meteocat readings (REST connector, parameterized) into `Files/raw/weather/` |
| `nb_06_weather_bronze` | Notebook | Raw weather JSON to `bronze.weather_xema` |
| `nb_07_weather_silver` | Notebook | Weather cleaning, time zone conversion, hourly aggregation and `gold.dim_weather` |
| `eh_bicing` | Eventhouse | KQL database with `station_status`, `dim_station` and `dim_weather`, a stored function and a materialized view |
| `nb_08_ds_exploration` | Notebook | Modeling dataset, exploration, baseline, time split and Data Wrangler |

The choice of engine for each piece is documented in [`docs/decision-table.md`](docs/decision-table.md).

## Data model

Star schema. The grain of the fact table is **one station per local hour** (Europe/Madrid).

| Table | Description |
|---|---|
| `fact_station_availability` | 4.562.937 rows. Readings, average, minimum and maximum bikes available, average docks available, and counts of empty, full and out-of-service readings per station and hour |
| `dim_station_history` | SCD type 2: 5.014 versions of 554 stations, with `station_key`, `valid_from`, `valid_to` and `is_current` |
| `dim_time` | 8.760 hours (one year), with date parts, weekday, day part and season |
| `dim_station` | Current-version station dimension (lakehouse, dataflow and warehouse copies) |
| `station_capacity` | 4.938 capacity ranges per station (`valid_from`, `valid_to`) |
| `dim_weather` | 8.760 hours: temperature, precipitation, wind speed and `n_readings` (number of half-hourly readings behind each hour). Lakehouse table, copied to the warehouse with `sql/06_dim_weather.sql` |

Station attributes change over time (name, address, capacity). The fact table points to the dimension version valid in each hour, so the occupancy rate is always computed with the capacity of that moment, not the latest one. Of the 4.460 changes between versions, 4.384 involve capacity.

In the warehouse, a view (`gold.v_station_current`), an inline table-valued function (`gold.fn_station_at`) and a stored procedure (`gold.usp_reload_star`, which reloads dimension and fact in a single transaction) sit on top of the model. The SQL scripts are in [`sql/`](sql/). Query performance was inspected with the `queryinsights` views and the dynamic management views.

## Eventhouse

The Eventhouse `eh_bicing` has one KQL database with three tables: `station_status` (4.489.506 rows), `dim_station` (554 rows) and `dim_weather` (8.760 rows), loaded from Parquet files exported from the lakehouse (`Files/export/`). On top sit a stored function (`hourly_occupancy`) and a materialized view (`hourly_station_mv`).

The 15 queries in [`kql/01_eventhouse_queries.kql`](kql/01_eventhouse_queries.kql) cover exploration (`take`, `getschema`), aggregation and time binning, rankings, joins, and the function and the materialized view.

## Data quality decisions

What the data looked like and what the project does about it.

**Volume**
- The station information files contain a snapshot of every station several times per hour. `scripts/hourly_info.py` keeps the first snapshot of each hour, which matches the grain of the fact table.

**Schema drift**
- The station information files from November 2024 to June 2025 carry an extra column (`V1`); from July 2025 it is gone. Files are loaded with `unionByName(allowMissingColumns=True)`, so `V1` is `NULL` in the later months.
- Several columns contain the literal text `NA`. In the October 2025 file, `altitude` has 13.754 of them (3,6 %) and `rental_uris` is `NA` in all 377.742 rows, so that column is not carried forward.

**Duplicates**
- Between 30 November 2024 and 30 June 2025, 4.156 station and timestamp pairs appear twice (8.312 rows). The two rows are identical in every data column and differ only in the source file.
- Deduplication uses the business key (`station_id`, `last_updated`) with `ROW_NUMBER` or `dropDuplicates`. `SELECT DISTINCT *` is not enough: it removes none, because `source_file` differs. Even excluding the lineage columns it leaves 534 extra rows at the June/July 2025 boundary, where `V1` has a value in one copy and `NULL` in the other.

**Time**
- The analysis hour is local time (Europe/Madrid). On 30 March 2025 the 02:00 hour does not exist (clocks go forward). On 26 October 2025 (clocks go back) the source has no reading for 00:00 UTC, so one of the two local hours labelled 02:00 is missing.
- The source has a gap on 20 May 2025 from 04:00 to 06:59 local time.
- October 2025 ends on 29 October at 21:55 local time (the portal note mentions the 28th), and November 2025 is not available. The fact table is limited to November 2024 to October 2025.

**Stations**
- 554 stations in total: 550 regular, 2 event stations (`VALET`: 542 and 543) and 2 test stations (536 and 545, identified by their names).
- 588 has capacity 0 and a single snapshot. Six stations have no district (597 among them) and are assigned to `Unassigned`.
- Stations 593 and 594 appear in the status data but not in the station information, so they are dropped from the fact table.
- Capacity changes often. Operational docks out of use is a hypothesis, not a confirmed cause.
- Capacity ranges start at the hour of the first snapshot of each version, and the first version of each station is extended back to 1900-01-01 so that no fact row falls outside a range.

**Types**
- In the warehouse, a plain `CTAS` from Spark strings creates `varchar(8000)` columns. Text columns are cast to the real maximum lengths instead.
- Staging tables keep everything as text. `CAST` of `'NA'` to a number fails in T-SQL and aborts the statement, while Spark's default `cast` returns `NULL`; `TRY_CAST` behaves the same in both.
- In a Dataflow Gen2, columns of type `Any` are silently not written to the lakehouse destination; every column needs an explicit type.

**Dataflow Gen2 and notebook**
- `df_dim_station` and `nb_03_station_info` produce the same `dim_station` (554 rows).

**Weather**
- Meteocat timestamps are UTC and mark the start of each half-hourly interval. They are converted to Europe/Madrid before aggregating to hours.
- Daylight saving time: the hour that does not exist on 30 March 2025 has no readings, and the repeated hour on 26 October 2025 is merged into a single local hour.
- Hours without any reading are kept with `NULL` values and `n_readings = 0`. `n_readings` is the smallest number of readings among the three variables in that hour.
- Temperature and wind are averaged per hour and precipitation is summed.
- The weather comes from one station (X4, el Raval), so every Bicing station shares the same weather values. This is a simplification, not a measurement at each station.

**Modeling dataset**
- Rows with `capacity = 0` (1.643 rows, 91 % of them followed by an empty reading) are excluded: the station is out of service, so "no bikes" is not a useful prediction target. Stations in the district `Unassigned` (536, 545, 588 and 597, 2.659 rows) are excluded too, as test, zero-capacity or otherwise special stations. In total 4.300 rows are removed.
- 19 regular stations have no altitude in the source. Their `altitude` stays `NULL` in the table and is filled with the median of the training set when the data is prepared.
- The first hour of each station has no `avg_bikes_lag1`, and the first 24 hours have no `avg_bikes_lag24`; those stay `NULL` as well.

## Predicting an empty station

**Question.** At the end of hour *t*, will the station have at least one reading with no bikes during hour *t+1*?

**Dataset.** `gold.ml_station_hour`: 4.556.003 rows, 548 stations, one row per station and hour. The label `empty_next` is 1 for 20,4 % of the rows. Features, all known at the end of hour *t*: bikes and docks available, empty and full readings in the hour, bikes available one and 24 hours earlier, hour of day, day of week, weekend flag, month, altitude, district, capacity, and the temperature, precipitation and wind of the hour.

**Time split.** The data is split by date, never at random, because the label looks one hour ahead:

| Table | Period | Rows | `empty_next` |
|---|---|---|---|
| `gold.ml_train` | 1 November 2024 to 31 August 2025 | 3.790.038 | 20,0 % |
| `gold.ml_validation` | September 2025 | 389.789 | 22,7 % |
| `gold.ml_test` | 1 to 29 October 2025 | 375.090 | 22,8 % |

The last hour of August and of September is dropped, because its label would fall in the next block. Autumn has a higher share of empty stations than the rest of the year, so the training set is not fully representative of the test period.

**Preparation.** The median altitude (26,0 m, computed per station) and the list of 10 districts are calculated on the training set only and reused for validation and test, so no information from the future leaks into the preparation. The function `preparar()` in `nb_08_ds_exploration` fills the altitude and turns the district into 10 columns of 0 and 1. It was first sketched with Data Wrangler on a sample of 50 stations from the training set.

**What the exploration shows** (share of rows with `empty_next = 1`):
- Hour of day: on working days there are two peaks, 29,2 % for hour 8 and 29,8 % for hour 17, and a low of 9,5 % at hour 5. At weekends there is no morning peak and the maximum is 27 to 28 % in the afternoon.
- Altitude: 4,5 % below 20 m, 24,7 % from 20 to 60 m, 41,7 % from 60 to 120 m and 47,5 % above 120 m. It is the strongest signal.
- Rain: 20,8 % with no rain, 13,3 % below 1 mm and 10,1 % from 1 mm. Fewer stations empty when it rains, probably because the service is used less.

**Baseline.** "If the station had an empty reading in hour *t*, predict that it will in *t+1*" gives accuracy 89,4 %, precision 0,741, recall 0,741 and F1 0,741 (always predicting "not empty" gives 79,6 % accuracy). Measured on the whole year for reference; a trained model has to beat the F1 on the test period.

## How to reproduce

1. Download the monthly files for station status and station information from the Open Data BCN portal (November 2024 to October 2025) into `data/raw/csv/status` and `data/raw/csv/info`.
2. Reduce the station information files to one snapshot per hour from the repository root:
   ```bash
   python scripts/hourly_info.py
   ```
3. Create a Fabric workspace with a trial capacity, connect it to this repository (Git integration, folder `fabric/`) and create `lh_bicing`.
4. Upload the raw status files and the reduced information files to `Files/raw` and `Files/bronze` of the lakehouse.
5. Run `pl_bicing_bronze`, then the notebooks in order: `nb_01`, `nb_02`, `nb_03`, `nb_04`.
6. Create `wh_bicing` and run the scripts in [`sql/`](sql/) in order (`01` to `05`). Scripts `02` and `05` need the workspace and lakehouse ids in the OneLake URLs. `COPY INTO` from OneLake is in public preview at the time of writing.
7. Weather: run `pl_weather_bronze` with the parameters `variables`, `station` (`X4`), `date_from` and `date_to`, then `nb_06_weather_bronze` and `nb_07_weather_silver`, and `sql/06_dim_weather.sql` in the warehouse. The pipeline run dialog adds a trailing `Z` to date parameters; the pipeline expression removes it.
8. Eventhouse: create `eh_bicing`, export the gold tables to Parquet in `Files/export/`, load them with *Get data* from OneLake, and run the queries in [`kql/`](kql/).
9. Run `nb_08_ds_exploration` to build the modeling dataset, the baseline and the three split tables.

## Repository structure

- `fabric/`: Fabric items synced through Git (notebooks, pipelines, dataflow, lakehouse, warehouse, Eventhouse)
- `sql/`: T-SQL scripts for the warehouse, in execution order
- `kql/`: KQL queries for the Eventhouse
- `scripts/`: local helper scripts (`hourly_info.py`)
- `docs/`: decision table, architecture diagram, data dictionary and screenshots

## Roadmap

- Activator alert design on the Eventhouse data
- Machine learning: train a model for "empty in the next hour" and compare it with the baseline on the test set
- Direct Lake semantic model and Power BI report

## Author

Xavi Peña · [GitHub](https://github.com/xavipena-data)