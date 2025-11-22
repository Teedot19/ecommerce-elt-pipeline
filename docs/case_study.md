# **Case Study: Production-Style Batch Data Pipeline**

## **Overview**

This case study presents a full, production-style batch data pipeline covering the entire lifecycle of a modern data platform. It includes synthetic data generation, validation, quarantining, transformation, cloud ingestion, warehouse automation, and orchestration.

---

## **Synthetic Data Generation**

Synthetic event records were generated using Faker to emulate a source system. Noise and inconsistencies were intentionally introduced, including:

* Incorrect data types
* Missing fields
* Malformed timestamps
* Out-of-range values

This ensured downstream logic performed real-world validation instead of relying on clean, unrealistic test data.

---

## **Ingestion and Validation Pipeline**

The ingestion layer uses a strict batch-processing model with schema enforcement.

### **Pydantic Schemas**

Schemas were defined using Pydantic to guarantee structured, validated inputs. Benefits include:

* Clear, strongly typed schemas
* Safe type coercion
* Fine-grained field validation
* Automatic separation of malformed records
* Cleaner and more testable transformations

### **Valid vs. Quarantine Routing**

All incoming records are partitioned into:

* **Valid records**
* **Invalid records (quarantine)**

Invalid data is retained for investigation and replay, preventing silent data loss and aligning with production best practices.

---

## **Testing for Reliability**

A testing suite was created using pytest, covering:

* Unit tests for transformation logic
* Batch wrapper tests
* End-to-end integration tests

The test suite ensures deterministic behavior and prevents regressions.

---

## **Cloud Storage Layer**

Validated outputs are exported to Google Cloud Storage (GCS). Folder layout mirrors production data lakes:

```
gs://bucket/
    raw/
    validated_raw/
    quarantine_raw/
```

This structure supports lineage, governance, and reprocessing workflows.

---

## **Snowflake Ingestion via Snowpipe**

A Snowflake external stage points to the GCS bucket.

### **Automated Ingestion**

New file arrivals trigger Snowpipe through Google Pub/Sub:

1. GCS event notifies Pub/Sub
2. Pub/Sub triggers Snowpipe
3. Snowpipe runs `COPY INTO` automatically

This establishes a low‑maintenance, near‑real‑time ingestion path into the bronze/raw warehouse layer.

---

## **dbt Transformation Layers**

Snowflake tables are transformed using dbt, following a standard warehouse modeling pattern.

### **Staging Layer**

* Cleansing
* Type normalization
* Source alignment

### **Intermediate Layer**

* Business logic
* Joins and aggregations

### **Marts Layer**

* Final analytics‑ready tables
* Domain‑specific datasets

This structure improves maintainability and modularity.

---

## **Dagster Orchestration**

Dagster orchestrates the system with:

* Asset definitions
* Execution coordination
* Dependency tracking
* Lineage visualisation
* Logging and observability

Dagster ties together ingestion, Snowpipe automation, and dbt model execution.

---

## **Summary**

This project implements a realistic, fully integrated data engineering system including:

* Synthetic data generation
* Pydantic‑driven validation
* Quarantine handling
* GCS data lake ingestion
* Automated Snowflake ingestion via Snowpipe
* Layered dbt transformations
* Dagster orchestration
* Comprehensive testing

The design reflects production‑grade patterns and software engineering discipline.
