# E-Commerce ELT Pipeline

**Python • Dagster • dbt • Google Cloud Storage • Snowflake**

## Table of Contents

1.  [Overview](#overview)
2.  [Architecture: High-Level Flow](#architecture-high-level-flow)
3.  [Features](#features)
    *   [Ingestion Layer (Python)](#ingestion-layer-python)
    *   [Orchestration (Dagster)](#orchestration-dagster)
    *   [Transformations (dbt)](#transformations-dbt)
    *   [CI/CD (GitHub Actions)](#cicd-github-actions)
4.  [Repository Structure](#repository-structure)
5.  [Data Layout in GCS](#data-layout-in-gcs)
6.  [Running the Pipeline](#running-the-pipeline)
    *   [Run Ingestion Manually](#run-ingestion-manually)
    *   [Run Dagster](#run-dagster)
    *   [dbt Lineage](#dbt-lineage)
7.  [CI Workflows](#ci-workflows)
    *   [Python CI](#python-ci)
    *   [dbt CI](#dbt-ci)
8.  [Engineering Principles](#engineering-principles)
9.  [What This Project Demonstrates](#what-this-project-demonstrates)

## Overview

This project implements a modular, production-style ELT pipeline. It ingests synthetic e-commerce data, validates it, stores it in Google Cloud Storage, and transforms it in Snowflake using dbt. The pipeline is orchestrated with Dagster and follows strict software engineering principles, including:

*   Single Responsibility Principle (SRP)
*   Don't Repeat Yourself (DRY)
*   Pure functions
*   Dependency injection
*   Full testability

This repository demonstrates modern data engineering practices end-to-end, making it suitable for portfolio projects and real-world workloads.

## Architecture: High-Level Flow

```
Data Generator (Faker) ↓ Raw CSV Output ↓ Ingestion Layer (Extract → Validate → Partition)
├── Validated Records → validated_raw/
├── Invalid Records → quarantine_raw/
└── Full Copy → raw/ ↓ GCS Storage (partitioned) ↓ Snowflake (Snowpipe or External Tables) ↓ dbt Models (Staging → Intermediate → Marts)
```

## Features

### Ingestion Layer (Python)

*   Entity-specific ingestion for customers, products, orders, order_items, and payments.
*   Pydantic-based schema validation.
*   Invalid rows are routed to a quarantine area with detailed error metadata.
*   Valid rows are written to `validated_raw`.
*   A full raw copy is preserved.
*   All I/O is isolated behind dependency-injected GCS loaders.
*   Designed for readability, testability, and maintainability.

### Orchestration (Dagster)

*   Daily partitions
*   Asset-based orchestration
*   Automatic triggering of dbt after ingestion
*   Lineage visualization
*   Arbitrary re-materialization and backfills
*   Clear separation of compute and definitions

### Transformations (dbt)

*   Snowflake-based ELT modeling
*   Layered schema:
    *   staging
    *   intermediate
    *   marts
*   Schema tests, relationship tests, and not-null checks
*   Macros for standardization
*   dbt Docs for lineage visualization

### CI/CD (GitHub Actions)

*   Python unit test workflow (pytest)
*   dbt CI workflow using Snowflake credentials from GitHub Secrets
*   Dependency caching for faster builds

## Repository Structure

```
src/batch_data_pipeline/
├── generators/           # synthetic data generator
├── ingestion/
│   ├── ingestors/       # per-entity ingestion (SRP enforced)
│   ├── loaders/          # GCS upload helpers (DI, pure functions)
│   └── validation/       # schemas + validation utilities
├── orchestration/
│   ├── assets/          # Dagster assets (ingestion, dbt)
│   └── definitions.py   # Dagster project definitions

ecommerce_analytics/     # dbt project
├── models/
├── macros/
├── snapshots/
└── dbt_project.yml

tests/                   # pytest suite
docs/                    # architecture diagrams, case studies
```

## Data Layout in GCS

| Layer            | Description                                  | Example Path                                               |
| ---------------- | -------------------------------------------- | ---------------------------------------------------------- |
| `raw/`           | Original generator output                      | `raw/ecommerce/2025-11-15/orders_2025-11-15.csv`           |
| `validated_raw/` | Rows passing Pydantic validation             | `validated_raw/orders/2025/11/15/orders.csv`                |
| `quarantine_raw/`| Rejected rows                                | `quarantine_raw/orders/2025/11/15/orders_bad.csv`           |

All layers are grouped by entity and partitioned by date.

## Running the Pipeline

### Run Ingestion Manually

```bash
poetry run python -m batch_data_pipeline.main_runner 2025-11-15
```

### Run Dagster

```bash
poetry run dagster dev
```

The Dagster UI will be available at:

```
http://localhost:3000
```

### dbt Lineage

```bash
cd ecommerce_analytics
dbt docs generate
dbt docs serve --port 8082
```

## CI Workflows

### Python CI

*   Runs unit tests with pytest
*   Installs dependencies via Poetry
*   Triggered on push and pull requests

### dbt CI

*   Builds the dbt project in Snowflake
*   Uses GitHub Secrets for authentication
*   Executes `dbt build --warn-error` and `dbt test`

## Engineering Principles

This project is intentionally designed to reflect real production engineering standards:

*   **Single Responsibility Principle (SRP):** Each ingestion module handles exactly one entity.
*   **Pure Functions:** Extract, validate, and transform logic are isolated from I/O.
*   **Dependency Injection:** GCS clients are passed explicitly.
*   **No Hidden State:** No implicit mutations or global clients.
*   **Fully Testable:** Every ingestion function has pytest coverage.
*   **Consistent Naming & Layout:** Predictable entity → file → schema mapping.
*   **Low Coupling:** Validation, loading, and orchestration are independent modules.

## What This Project Demonstrates

*   End-to-end ELT orchestration
*   Cloud-native data platform design
*   Clean and maintainable Python pipelines
*   dbt modeling and testing patterns
*   Dagster asset-based orchestration
*   CI-driven reliability
*   Partitioned ingestion into GCS & Snowflake

