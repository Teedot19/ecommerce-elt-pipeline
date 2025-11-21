Case Study: Building a Production-Style Batch Data Pipeline
Overview

This project implements a complete, production-style batch data pipeline that simulates the full lifecycle of a modern data platform. It includes synthetic data generation, validation, quarantining, transformation, cloud storage ingestion, automated warehouse loading, and orchestration. The goal was to design a realistic system that mirrors how contemporary data teams operate in high-scale environments.

1. Synthetic Data Generation

To act as the source system provider, I generated synthetic events using Faker, a Python library that produces realistic records. I intentionally introduced noisy, inconsistent, and malformed values to replicate typical real-world data quality problems such as:

incorrect types

missing values

malformed timestamps

out-of-range measurements

This ensured the downstream ingestion logic had to perform actual validation rather than relying on clean test data.

2. Ingestion and Validation Pipeline

The ingestion layer uses a batch processing model with strict schema enforcement.

2.1 Pydantic Schemas

I used Pydantic to define strongly typed schemas for each stage. This ensured every transformation step received structured, validated, and predictable inputs.

Pydantic provided several benefits:

schema correctness and clarity

automatic type coercion where safe

strict field-level validation rules

easy quarantining of malformed events

cleaner, safer, testable transformations

2.2 Valid vs Quarantine Routing

Incoming records are partitioned into:

valid_records

invalid_records (quarantine)

Invalid data is never deleted. It is preserved for investigation, debugging, and potential replay. This aligns with production best practices and prevents silent data loss.

3. Testing for Reliability

To ensure correctness and prevent regressions, I wrote:

unit tests for individual transformations

batch wrapper tests

integration tests for end-to-end pipeline execution

Tests were written using pytest and cover model-level behavior, edge cases, and the pipeline runner.

This testing strategy ensures the pipeline is deterministic and refactor-safe.

4. Cloud Storage Layer

Validated records are exported to Google Cloud Storage (GCS).

The output folder structure mimics real production data lakes:

gs://bucket/
  raw/
  validated_raw/
  quarantine_raw/


This separation supports lineage tracking, governance, and reprocessing.

5. Snowflake Ingestion via Snowpipe

To simulate a production-ready warehouse, I used Snowflake with an external stage pointing to the GCS bucket.

5.1 Automated Ingestion

Snowflake ingests new files using Snowpipe, triggered through Google Pub/Sub.
Whenever new data lands in GCS:

Pub/Sub notifies Snowflake

Snowpipe executes a COPY INTO command

Data is loaded into the bronze/raw layer

This creates a near-real-time, low-maintenance ingestion path.

6. dbt Transformation Layers

Data is transformed in Snowflake using dbt, following the standard warehouse modeling pattern:

6.1 Staging Layer

source cleaning

type alignment

schema normalization

6.2 Intermediate Layer

business logic

joins

aggregations

6.3 Marts Layer

analytics-ready models

domain-specific datasets

final tables consumed by dashboards

This approach keeps transformations modular, testable, and maintainable.

7. Dagster Orchestration

I used Dagster to orchestrate the entire pipeline.
Dagster provides:

data asset definitions

execution orchestration

dependency management

lineage visualization

observability and logging

The orchestration layer coordinates ingestion, Snowpipe processing, and dbt model execution.


8. Summary

This project delivers a realistic, end-to-end data engineering system that covers:

synthetic data generation

validation using Pydantic

quarantine handling

cloud data lake ingestion

Snowflake auto-ingestion

multi-layer dbt modeling

pipeline orchestration with Dagster

robust testing and schema guarantees

It demonstrates both software engineering rigor and production-grade data engineering design.