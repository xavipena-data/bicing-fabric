# Decision table: lakehouse, warehouse or eventhouse

Where each piece of the Bicing project lives and why.

| Piece | Choice | Why | Status |
|---|---|---|---|
| Monthly CSV files as published by the portal | Lakehouse (Files) | Raw files kept as they arrive, any format, no schema needed | Built |
| Bronze and silver Delta tables | Lakehouse | Cleaning, typing and deduplication are done in PySpark; Delta is the native format | Built |
| Gold fact and dimensions for reporting | Lakehouse | Built in Spark; Direct Lake reads the Delta tables directly | Built |
| Station dimension with history (SCD type 2), queried and updated by analysts | Warehouse | Full T-SQL, row-level updates and multi-table transactions | Planned (week 3) |
| Live station status, a new reading every few seconds | Eventhouse | High-volume continuous ingestion, time-series queries in KQL with low latency | Planned (week 4) |
| Alert when a station has had no bikes for 30 minutes | Activator | Evaluates a condition on the stream and triggers an action; an eventhouse only stores and queries | Design only |
| Features and experiments for the empty-station model | Lakehouse + Data Science | Notebooks, MLflow tracking, Delta tables for predictions | Planned (weeks 4-5) |

## Rule of thumb

- Spark, notebooks, Python or raw files: **lakehouse**.
- SQL team, row updates, transactions across tables: **warehouse**.
- Events arriving continuously, time-series, KQL: **eventhouse**.
- React to a condition on a stream: **Activator**.