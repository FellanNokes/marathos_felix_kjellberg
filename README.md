# Marathos Data Platform

A data engineering project built for Marathos, a global marathon hosting company. The platform ingests, cleans and models ultra-marathon race data to enable data-driven decisions for business stakeholders. The platform is built on a streaming architecture, designed to continuously process new race data as it arrives.

## Tech Stack

- Databricks
- Python
- PySpark
- Plotly
- Git / GitHub

## Architecture

The platform follows a medallion architecture with three layers:

- **Bronze** - Raw data ingestion from CSV files using streaming tables
- **Silver** - Cleaned and transformed One Big Table (OBT)
- **Gold** - Dimensional model with fact and dimension tables and materialized views

## Project Structure
```
marathos_felix_kjellberg
├── dimensional_modeling
├── explorations
├── transformations
│   ├── bronze/
│   ├── silver/
│   └── gold/
└── utils/
```

## Data

The dataset contains two centuries of ultra-marathon race results including athlete performance, demographics and event information. Fake data for 2022-2025 was generated to simulate streaming ingestion.

## Dashboard

The Databricks dashboard provides an overview of race statistics and a country deep dive page. It is connected to Marathos Genie, an AI assistant that allows business stakeholders to query the data in natural language.
