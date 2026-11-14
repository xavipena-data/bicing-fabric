# Bicing Station Availability on Microsoft Fabric

An end-to-end analytics project built on Microsoft Fabric, seen through the eyes of a Bicing operator: which stations run empty or full, at what times, and where.

> **Status:** the Fabric build is complete: lakehouse (medallion), warehouse, Eventhouse, the machine learning model, a Direct Lake semantic model and a four-page Power BI report. Still to come: the Git workflow, a deployment pipeline and row-level security.

## Goal

Answer one operational question: *where and when does the service fail to offer a bike or a free dock?* The analysis covers daily and weekly patterns and seasonality over almost a full year of data (1 November 2024 to 29 October 2025, the last day available).

## Stack

- Microsoft Fabric: Lakehouse, Warehouse, Eventhouse (KQL), Data Pipelines, Notebooks (Spark), Dataflow Gen2, Data Science (MLflow experiments and model registry), Direct Lake semantic model, Power BI
- Medallion architecture (bronze, silver, gold)
- T-SQL, KQL, PySpark and DAX
- LightGBM and MLflow for the prediction model
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

One lakehouse (`lh_bicing`) holds the medallion layers as schemas. A warehouse (`wh_bicing`) holds the gold model for SQL consumers, and an Eventhouse (`eh_bicing`) holds a copy of the data for KQL analysis. A Direct Lake semantic model (`sm_bicing`) reads the gold tables of the lakehouse, and a Power BI report (`rpt_bicing`) sits on top of it.

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
| `nb_09_ml_training` | Notebook | Model training and comparison, logged to MLflow |
| `exp_bicing_empty_next` | Experiment | MLflow runs: baseline, LightGBM configurations and the final test evaluation |
| `ml_bicing_empty_next` | ML model | Registered LightGBM classifier (version 1) |
| `nb_10_batch_predictions` | Notebook | Batch scoring of the validation and test periods into `gold.station_predictions` |
| `nb_11_exclude_stations` | Notebook | Final adjustments to the gold tables and the `district_label` column of `dim_station` |
| `sm_bicing` | Semantic model | Direct Lake model with the measures used by the report |
| `rpt_bicing` | Report | Four-page Power BI report: Overview, Where, When and Model |

The choice of engine for each piece is documented in [`docs/decision-table.md`](docs/decision-table.md).

## Data model

Star schema. The grain of the fact table is **one station per local hour** (Europe/Madrid).

| Table | Description |
|---|---|
| `fact_station_availability` | 4.560.265 rows. Readings, average, minimum and maximum bikes available, average docks available, and counts of empty, full and out-of-service readings per station and hour |
| `dim_station_history` | Warehouse only. SCD type 2: 5.006 versions of 548 stations, with `station_key`, `valid_from`, `valid_to` and `is_current` |
| `dim_time` | 8.760 hours (one year), with date parts, weekday, day part and season |
| `dim_station` | Current-version station dimension, 548 stations (lakehouse and warehouse copies). The lakehouse copy also has `district_label`, the official district name |
| `station_capacity` | 4.938 capacity ranges per station (`valid_from`, `valid_to`) |
| `dim_weather` | 8.760 hours: temperature, precipitation, wind speed and `n_readings` (number of half-hourly readings behind each hour). Lakehouse table, copied to the warehouse with `sql/06_dim_weather.sql` |
| `station_predictions` | Predictions of the model for the validation and test periods: probability, predicted label (`empty_predicted`) and actual label (`empty_actual`) per station and hour |

Station attributes change over time (name, address, capacity). The fact table points to the dimension version valid in each hour, so the occupancy rate is always computed with the capacity of that moment, not the latest one.

In the warehouse, a view (`gold.v_station_current`), an inline table-valued function (`gold.fn_station_at`) and a stored procedure (`gold.usp_reload_star`, which reloads dimension and fact in a single transaction) sit on top of the model. The SQL scripts are in [`sql/`](sql/). Query performance was inspected with the `queryinsights` views and the dynamic management views.

## Eventhouse

The Eventhouse `eh_bicing` has one KQL database with three tables: `station_status` (4.480.932 rows: the readings of 1 to 29 October 2025 for 545 stations), `dim_station` (548 rows) and `dim_weather` (8.760 rows), loaded from Parquet files exported from the lakehouse (`Files/export/`). On top sit a stored function (`hourly_occupancy`) and a materialized view (`hourly_station_mv`).

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
- The station information lists 554 stations. Stations 593 and 594 appear in the status data but not in the station information, so they are dropped from the fact table.
- Capacity changes often. Operational docks out of use is a hypothesis, not a confirmed cause.
- Capacity ranges start at the hour of the first snapshot of each version, and the first version of each station is extended back to 1900-01-01 so that no fact row falls outside a range.
- 28 April 2025 (the Iberian blackout) shows a spike of out-of-service readings: 30,12 % of that day's readings. It is a real event and it stays in the data.

**Types**
- In the warehouse, a plain `CTAS` from Spark strings creates `varchar(8000)` columns. Text columns are cast to the real maximum lengths instead.
- Staging tables keep everything as text. `CAST` of `'NA'` to a number fails in T-SQL and aborts the statement, while Spark's default `cast` returns `NULL`; `TRY_CAST` behaves the same in both.
- In a Dataflow Gen2, columns of type `Any` are silently not written to the lakehouse destination; every column needs an explicit type.

**Dataflow Gen2 and notebook**
- `df_dim_station` and `nb_03_station_info` produce the same `dim_station` (554 rows at the time of the comparison).

**Weather**
- Meteocat timestamps are UTC and mark the start of each half-hourly interval. They are converted to Europe/Madrid before aggregating to hours.
- Daylight saving time: the hour that does not exist on 30 March 2025 has no readings, and the repeated hour on 26 October 2025 is merged into a single local hour.
- Hours without any reading are kept with `NULL` values and `n_readings = 0`. `n_readings` is the smallest number of readings among the three variables in that hour.
- Temperature and wind are averaged per hour and precipitation is summed.
- The weather comes from one station (X4, el Raval), so every Bicing station shares the same weather values. This is a simplification, not a measurement at each station.

**Modeling dataset**
- Rows with `capacity = 0` (1.643 rows, 91 % of them followed by an empty reading) are excluded: the station is out of service, so "no bikes" is not a useful prediction target. Stations in the district `Unassigned` (2.659 rows) are excluded too, as special stations. In total 4.300 rows are removed.
- 19 regular stations have no altitude in the source. Their `altitude` stays `NULL` in the table and is filled with the median of the training set when the data is prepared.
- The first hour of each station has no `avg_bikes_lag1`, and the first 24 hours have no `avg_bikes_lag24`; those stay `NULL` as well.

## Predicting an empty station

**Question.** At the end of hour *t*, will the station have at least one reading with no bikes during hour *t+1*?

**Dataset.** `gold.ml_station_hour`: 4.556.003 rows, 548 stations, one row per station and hour. The label `empty_next` is 1 for 20,4 % of the rows. Features, all known at the end of hour *t*: bikes and docks available, empty and full readings in the hour, bikes available one and 24 hours earlier, hour of day, day of week, weekend flag, altitude, district, capacity, and the temperature, precipitation and wind of the hour. The month is not a feature: with a single year of data, the months of the validation and test sets are never seen in training.

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

**Baseline.** "If the station had an empty reading in hour *t*, predict that it will in *t+1*." It gives an F1 of 0,7516 on the validation set and 0,7539 on the test set. A trained model has to beat the baseline on the same period.

**Model.** A LightGBM classifier. The configurations were compared on the validation set in the MLflow experiment `exp_bicing_empty_next`; the best one (`lgbm_v3_grande`) was evaluated once on the test set and registered as `ml_bicing_empty_next` (version 1). The decision threshold is 0,3555.

| | F1, validation | F1, test |
|---|---|---|
| Persistence baseline | 0,7516 | 0,7539 |
| LightGBM (`lgbm_v3_grande`) | 0,7820 | 0,7843 |

The model beats the baseline by about 3 points of F1 on both sets. The gain is real but modest: much of the signal is already in the last reading of the station. `nb_10_batch_predictions` scores the validation and test periods and stores the result in `gold.station_predictions`, so the report can show it.

## Semantic model

`sm_bicing` is a Direct Lake semantic model on OneLake. It reads the gold tables `fact_station_availability`, `dim_station`, `dim_time`, `dim_weather` and `station_predictions` of the lakehouse, without importing data.

- **Measures** live in a `_Measures` table, in two display folders: *Availability* (readings, empty, full and out-of-service rates, average bikes and docks available, active stations, empty rate in dry and rainy hours) and *Model* (confusion matrix counts, precision, recall, F1 and accuracy). The empty rate is calculated over all readings (empty readings divided by readings), not as an average of averages.
- **Calculation group** `Network Comparison` with three items: *Actual*, *Δ vs Network* and *Index vs Network*, with dynamic format strings. The network baseline stays the whole network when a district is filtered.
- **Field parameter** `Metric Selector` over five availability metrics, so one visual can show any of them.
- The model metrics carry no filter on the dataset split: the report has a slicer (*Dataset split*) and shows the test set by default.
- Direct Lake does not allow calculated columns, so every derived value is a measure.

## Power BI report

`rpt_bicing` has four pages. It follows a black sidebar, light canvas and yellow accent style.

| Page | What it shows |
|---|---|
| Overview | KPI cards for the whole network, a metric selector, the chosen metric by month (line chart) and by district (bar chart) |
| Where | Map of the stations (Azure Maps), districts against the network average, and the top 10 stations by empty rate |
| When | Heat matrix of the empty rate by hour and weekday, weekdays against weekends by hour, and dry against rainy hours by hour |
| Model | Model card, precision, recall, F1 and accuracy, confusion matrix, predicted against actual empty stations, and precision and recall by hour |

The district and month slicers are synchronized across *Where* and *When*. Screenshots of each page are in [`docs/images`](docs/images).

**What the report shows.** Across the whole period, the empty rate is 10,20 % of readings: 10,42 % on weekdays and 9,65 % at weekends. The top 10 of stations by empty rate has no minimum number of readings, on purpose: it is meant for someone who wants the real situation of the stations, so recently opened stations with few readings appear too. The month line chart shows the 28 April 2025 blackout as a spike in out-of-service readings (see [Data quality decisions](#data-quality-decisions)).

**Performance.** Measured with the Performance analyzer on the first load of each page (cold cache): every visual renders in less than 1,6 s. For the two heaviest visuals the DAX query takes about 60 ms and drawing takes about 25 ms; the rest of the time is waiting. No DirectQuery fallback appears in the breakdown.

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
10. Run `nb_09_ml_training` (trains and compares the models, logs them to MLflow and registers the best one), then `nb_10_batch_predictions` (creates `gold.station_predictions`), then `nb_11_exclude_stations`.
11. Apply the same station exclusion to the other two copies of the gold model: run [`sql/07_exclude_stations.sql`](sql/07_exclude_stations.sql) in `wh_bicing` and [`kql/02_exclude_stations.kql`](kql/02_exclude_stations.kql) in `eh_bicing`, one command at a time. The figures in this README are the ones you get after this step.
12. The semantic model `sm_bicing` and the report `rpt_bicing` arrive with the Git sync. Check that the semantic model points to `lh_bicing` of the new workspace, refresh it and open the report.

## Repository structure

- `fabric/`: Fabric items synced through Git (notebooks, pipelines, dataflow, lakehouse, warehouse, Eventhouse, ML experiment and model, semantic model and report)
- `sql/`: T-SQL scripts for the warehouse, in execution order
- `kql/`: KQL queries for the Eventhouse
- `scripts/`: local helper scripts (`hourly_info.py`)
- `docs/`: decision table, architecture diagram, data dictionary and screenshots

## Roadmap

- Git workflow: branches and merges for Fabric items
- Deployment pipeline from development to production, with impact analysis
- Row-level security by district in the semantic model, and column-level security in the warehouse

## Credits

The sidebar photo of the report is by Adrien Olichon, from [Pexels](https://www.pexels.com/).

## Author

Xavi Peña · [GitHub](https://github.com/xavipena-data)
