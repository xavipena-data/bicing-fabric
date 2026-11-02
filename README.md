# Bicing Station Availability on Microsoft Fabric

An end-to-end analytics project built on Microsoft Fabric, seen through the eyes of a Bicing operator: which stations run empty or full, at what times, and where.

> **Status:** work in progress.

## Goal

Answer one operational question: *where and when does the service fail to offer a bike or a free dock?* The analysis covers daily and weekly patterns and seasonality over a full year of data.

## Stack

- Microsoft Fabric: Lakehouse, Data Pipelines, Notebooks (Spark), Power BI
- Medallion architecture (bronze, silver, gold)
- Git integration for Fabric items

## Data

Station status and station information for Barcelona's Bicing service, published by Open Data BCN (Barcelona City Council):
[Bicing stations status](https://opendata-ajuntament.barcelona.cat/data/en/dataset/estat-estacions-bicing)

Licensed under [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/). The raw files are not versioned in this repository (`data/` is ignored); download instructions will be added to this README.

## Repository structure

- `fabric/`: Fabric items synced through Git (notebooks, pipelines, reports)
- `docs/`: architecture diagram, data dictionary and screenshots

## Author

Xavi Peña · [GitHub](https://github.com/xavipena-data)