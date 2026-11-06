# Bicing Station Availability on Microsoft Fabric

An end-to-end analytics project built on Microsoft Fabric, seen through the eyes of a Bicing operator: which stations run empty or full, at what times, and where.

> **Status:** work in progress. Lakehouse (medallion) and warehouse layers are built. Real-time, machine learning and reporting layers are planned.

## Goal

Answer one operational question: *where and when does the service fail to offer a bike or a free dock?* The analysis covers daily and weekly patterns and seasonality over a full year of data (1 November 2024 to 31 October 2025).

## Stack

- Microsoft Fabric: Lakehouse, Warehouse, Data Pipelines, Notebooks (Spark), Dataflow Gen2, Power BI
- Medallion architecture (bronze, silver, gold)
- T-SQL and PySpark
- Git integration for Fabric items

## Data

Station status and station information for Barcelona's Bicing service, published by Open Data BCN (Barcelona City Council):
[Bicing stations status](https://opendata-ajuntament.barcelona.cat/data/en/dataset/estat-estacions-bicing)

Licensed under [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/). The raw files are not versioned in this repository (`data/` is ignored); see [How to reproduce](#how-to-reproduce).

## Architecture

One lakehouse (`lh_bicing`) holds the medallion layers as schemas. A warehouse (`wh_bicing`) holds the gold model for SQL consumers.

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

Station attributes change over time (name, address, capacity). The fact table points to the dimension version valid in each hour, so the occupancy rate is always computed with the capacity of that moment, not the latest one. Of the 4.460 changes between versions, 4.384 involve capacity.

In the warehouse, a view (`gold.v_station_current`), an inline table-valued function (`gold.fn_station_at`) and a stored procedure (`gold.usp_reload_star`, which reloads dimension and fact in a single transaction) sit on top of the model. The SQL scripts are in [`sql/`](sql/).

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

## Repository structure

- `fabric/`: Fabric items synced through Git (notebooks, pipeline, dataflow, lakehouse, warehouse)
- `sql/`: T-SQL scripts for the warehouse, in execution order
- `scripts/`: local helper scripts (`hourly_info.py`)
- `docs/`: decision table, architecture diagram, data dictionary and screenshots

## Roadmap

- Eventhouse with KQL for live station status, and an Activator alert design
- Machine learning: predicting whether a station will be empty or full in the next hour
- Direct Lake semantic model and Power BI report

## Author

Xavi Peña · [GitHub](https://github.com/xavipena-data)