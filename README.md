# StreamGuard — Streaming Transaction Monitoring & Analytics

StreamGuard is an end-to-end data engineering portfolio project that captures transaction changes using Change Data Capture (CDC), processes events through a Bronze–Silver–Gold data lakehouse, and builds analytics-ready datasets for transaction and merchant performance monitoring.

The project brings together event streaming, distributed data processing, Delta Lake, dimensional modeling, data quality validation, and workflow orchestration in a locally runnable Docker environment.

**Project focus:** Reliable data pipelines, CDC ingestion, lakehouse architecture, analytical data modeling, reconciliation, and orchestration.

> **Important:** StreamGuard uses synthetic transaction data. High-value transaction flags and merchant review rankings are transparent, rule-based indicators. They are not confirmed fraud labels, and the project does not claim to use a trained fraud-detection model.

## Table of Contents

- [Architecture](#architecture)
- [Technology Stack](#technology-stack)
- [How the Pipeline Works](#how-the-pipeline-works)
- [Gold Data Model](#gold-data-model)
- [Data Quality and Reconciliation](#data-quality-and-reconciliation)
- [Airflow Orchestration](#airflow-orchestration)
- [Local Infrastructure](#local-infrastructure)
- [Run the Gold Pipeline](#run-the-gold-pipeline)
- [Project Structure](#project-structure)
- [Verified Local Results](#verified-local-results)
- [Engineering Principles](#engineering-principles)
- [Future Improvements](#future-improvements)

## Architecture

```text
                         Source Data
                Customers, Merchants, Transactions
                              |
                              v
                         PostgreSQL
                              |
                              v
                         Debezium CDC
                              |
                              v
                            Kafka
                              |
                              v
               Spark Structured Streaming
                              |
                              v
                    Bronze Layer — Parquet
                              |
                              v
                    Silver Layer — Delta
                              |
                              v
                     Gold Layer — Delta
                       /             \
                      v               v
               Gold Star Schema   Merchant Performance Mart
                       \             /
                        v           v
                    Automated Data Quality Checks
                              |
                              v
                      Apache Airflow DAG
```

The source-to-lakehouse pipeline uses CDC events to propagate source changes into the Bronze and Silver layers. Spark jobs then build Gold analytical datasets, while Airflow orchestrates the Gold build and validation sequence.

## Technology Stack

| Technology | Role in StreamGuard |
|---|---|
| Python | Data generation, event processing, and pipeline logic |
| PostgreSQL | Relational source data and supporting SQL tables |
| Debezium | Captures database changes through CDC |
| Apache Kafka | Transports CDC events |
| Apache Spark Structured Streaming | Processes streaming data |
| Delta Lake | Transactional storage for Silver and Gold datasets |
| Apache Airflow | Orchestrates and monitors Gold pipeline tasks |
| Docker Compose | Runs the local infrastructure |
| PowerShell | Runs the local Gold pipeline script on Windows |
| Git | Version control |

## How the Pipeline Works

### 1. Source Data and Change Data Capture

StreamGuard generates synthetic customers, merchants, and retail transactions. PostgreSQL stores the source records, and Debezium captures database inserts, updates, and deletes and publishes change events to Kafka.

This CDC approach allows downstream processing to respond to changes instead of relying exclusively on periodic full-table extraction.

### 2. Bronze Layer — Raw Ingestion

The Bronze layer stores incoming CDC data as Parquet files.

It contains separate datasets for:

- Transactions
- Customers
- Merchants

Separate Spark Structured Streaming writers and checkpoints are used for the relevant datasets. Keeping the ingestion layer separate from later transformations helps preserve a usable source-event history for downstream processing and troubleshooting.

### 3. Silver Layer — Processed Current State

The Silver layer stores processed datasets in Delta Lake.

CDC events are decoded and transformed to maintain the current-state representation of transactions, customers, and merchants. The Silver datasets provide the structured inputs for the Gold analytical models.

### 4. Gold Layer — Analytics and Dimensional Modeling

The Gold layer converts Silver data into business-oriented datasets.

It includes a dimensional star schema and a merchant performance mart. Gold tables are rebuilt using overwrite-based transformations designed to be repeatable, and validation scripts check the results.

### 5. Merchant Performance Analytics

The merchant performance mart aggregates transactions at the **merchant-and-currency grain**.

It calculates transaction volume, customer counts, approval and decline metrics, monetary summaries, high-value INR metrics, event-time ranges, and review-priority ranks.

Currency is part of the aggregation grain. As a result, transaction amounts in different currencies are not combined into one monetary total.

## Gold Data Model

### Star Schema

| Table | Purpose |
|---|---|
| `dim_customer` | Customer dimension |
| `dim_merchant` | Merchant dimension |
| `dim_date` | Calendar date dimension |
| `fact_transactions` | Transaction-level analytical facts |

The fact table contains transaction measures and foreign keys to the customer, merchant, and date dimensions. Validation ensures that the fact references existing dimension keys.

### Merchant Performance Mart

**Table:** `mart_merchant_performance`

Each row represents one unique merchant-and-currency combination.

The mart includes:

| Metric or field | Purpose |
|---|---|
| `transaction_count` | Number of transactions in the group |
| `unique_customer_count` | Number of distinct customers |
| `approved_count` | Transactions with approved status |
| `declined_count` | Transactions with declined status |
| `other_status_count` | Transactions with other or unrecognized statuses |
| `approval_rate_pct` | Percentage of approved transactions |
| `decline_rate_pct` | Percentage of declined transactions |
| `total_amount` | Total transaction amount in that currency |
| `average_transaction_amount` | Average transaction amount |
| `minimum_transaction_amount` | Minimum transaction amount |
| `maximum_transaction_amount` | Maximum transaction amount |
| `high_value_inr_count` | Count of high-value INR transactions |
| `high_value_inr_amount` | Amount attributable to high-value INR transactions |
| `first_event_time` | Earliest transaction event time in the group |
| `last_event_time` | Latest transaction event time in the group |
| `review_priority_rank` | Rule-based review ordering within a currency |

#### Review Ranking Rules

A merchant-and-currency group must contain at least **20 transactions** to qualify for ranking.

Eligible groups are ordered by:

1. Higher decline rate
2. Higher count of high-value INR transactions
3. Higher transaction count
4. Merchant key as a deterministic tie-breaker

Ranks are assigned independently within each currency. Groups that do not meet the minimum transaction threshold remain unranked.

The high-value INR threshold is ₹50,000. This is a configurable business-rule indicator used for review prioritization, not a determination that a transaction is fraudulent.

## Data Quality and Reconciliation

StreamGuard includes separate validation scripts for the Gold star schema and the merchant performance mart. These checks are run automatically as part of the pipeline.

### Gold Star-Schema Validation

The Gold validator checks:

- Primary keys are non-null and unique in each dimension and fact table.
- Required fact keys, amount, currency, and transaction status are not null.
- Transaction amounts are non-negative.
- Currency and transaction status are not blank or whitespace-only.
- Every fact customer key exists in `dim_customer`.
- Every fact merchant key exists in `dim_merchant`.
- Every fact date key exists in `dim_date`.

### Merchant Performance Mart Validation

The merchant mart validator checks:

- Each merchant-and-currency combination appears only once.
- Required merchant and currency identifiers are present.
- The sum of `transaction_count` equals the number of Gold fact transactions.
- Merchant-and-currency groups match those represented in the Gold fact table.
- Monetary totals reconcile separately for each currency.
- Approved, declined, and other status counts add up to each group's transaction count.
- Transaction and unique-customer counts are valid and internally consistent.
- Monetary aggregates are present and internally consistent.
- Ranked merchants meet the configured minimum transaction threshold.
- Every eligible group is ranked.
- Assigned ranks are positive and unique within each currency.

These checks go beyond checking whether a Spark job completed: they also test the integrity and consistency of the data it produced.

## Airflow Orchestration

The Airflow DAG is named `streamguard_gold_pipeline`.

It runs the following four tasks in sequence:

```text
build_gold
    |
    v
validate_gold
    |
    v
build_merchant_performance
    |
    v
validate_merchant_performance
```

The DAG uses task retries and logs Spark execution output. When a Spark script fails, the task error includes the exit code and a portion of the Spark output to aid diagnosis. Start and successful completion messages make task logs easier to scan.

The orchestration runs Spark scripts inside the existing `streamguard-spark` container.

## Local Infrastructure

The main Docker Compose configuration contains these services:

| Service | Purpose |
|---|---|
| `postgres` | Relational database |
| `debezium` | CDC connector |
| `kafka` | Event streaming |
| `spark` | Spark processing environment |
| `pgadmin` | PostgreSQL administration interface |
| `kafka-ui` | Kafka monitoring interface |
| `debezium-ui` | Debezium connector interface |

Airflow runs separately using `airflow/compose.yaml`. It connects to the StreamGuard Docker network and interacts with the running Spark container.

The repository also includes `sql/001_event_ledger.sql`, which defines supporting PostgreSQL tables for tracking Kafka events and recording fraud-alert records, including uniqueness and threshold constraints.

## Run the Gold Pipeline

These instructions assume Docker Desktop is installed and the repository has been checked out and configured.

### Prerequisites

- Windows with PowerShell
- Docker Desktop and Docker Compose
- The StreamGuard repository
- The project's Docker Compose configurations and supporting files

### 1. Start the Main Infrastructure

From the project root, `C:\StreamGuard`, run:

```powershell
docker compose up -d
```

This starts the services defined in the main Compose configuration.

### 2. Run the Gold Pipeline Manually

Run:

```powershell
.\scripts\run_gold_pipeline.ps1
```

The script runs these Spark jobs sequentially:

1. Build the Gold star schema.
2. Validate the Gold star schema.
3. Build the merchant performance mart.
4. Validate the merchant performance mart.

The script stops if a step fails and returns a non-zero exit code. A successful run ends with:

```text
Gold pipeline completed successfully.
```

### 3. Run Through Airflow

With the main StreamGuard infrastructure running, start Airflow:

```powershell
docker compose -f .\airflow\compose.yaml up -d
```

Open the Airflow UI:

**http://localhost:8082**

Select `streamguard_gold_pipeline` and trigger a DAG run. The task views show the status and logs for each of the four tasks.

## Run the End-to-End Transaction Demo

StreamGuard includes a lightweight demo that inserts a transaction
into PostgreSQL and lets you verify that it reaches the Gold lakehouse.

### Prerequisites

- The main Docker services are running.
- The Bronze writers for transactions, customers, and merchants are running.
- The Silver writer and Gold incremental writer are running.

Starting Docker Compose alone does not automatically start the
manually launched Spark streaming applications. Use the startup
instructions in `docs/commands_to_know.md` when bringing the
streaming pipeline online.

### 1. Insert a Test Transaction

From the project root, run:

```powershell
.\scripts\demo_transaction.ps1

## Project Structure

```text
StreamGuard/
├── airflow/
│   ├── Dockerfile
│   ├── compose.yaml
│   └── dags/
│       └── gold_pipeline.py
├── config/
├── data/
├── docs/
│   └── commands_to_know.md
├── lake/
├── logs/
├── scripts/
│   ├── check_streamguard.ps1
│   ├── demo_transaction.ps1
│   └── run_gold_pipeline.ps1
├── sql/
│   └── 001_event_ledger.sql
├── src/
│   ├── generator/
│   ├── storage/
│   ├── streaming/
│   │   ├── bronze_writer.py
│   │   ├── consumer.py
│   │   ├── decoder.py
│   │   └── processor.py
│   ├── silver/
│   │   ├── silver_writer.py
│   │   └── validate_silver.py
│   └── gold/
│       ├── build_gold.py
│       ├── build_merchant_performance.py
│       ├── gold_incremental_writer.py
│       ├── show_latest_transactions.py
│       ├── validate_gold.py
│       └── validate_merchant_performance.py
├── tests/
├── compose.yaml
├── pyproject.toml
└── uv.lock
```

The `data/`, `lake/`, and `logs/` directories are used for local data, lakehouse storage, and runtime output. Depending on the current Git ignore rules, generated and runtime files may not be tracked by Git.

## Verified Local Results

In a recent successful run, the Gold layer contained:

| Dataset | Row count |
|---|---:|
| `dim_customer` | 10,000 |
| `dim_merchant` | 500 |
| `dim_date` | 5 |
| `fact_transactions` | 100,010 |
| `mart_merchant_performance` | 501 |

The merchant mart contained 500 ranked rows and one unranked row in that run. All four tasks in the Airflow pipeline completed successfully, and the Gold and merchant mart validation checks passed.

These figures are a **snapshot of one local run**, not fixed guarantees. Counts can change when source records, event history, or transaction data change.

## Engineering Principles

StreamGuard emphasizes the following data-engineering practices:

- **CDC-based ingestion:** Captures changes from source tables.
- **Layered lakehouse design:** Separates ingestion, processed state, and analytics.
- **Repeatable transformations:** Gold tables can be rebuilt by the pipeline.
- **Data integrity:** Validates keys, relationships, business rules, and aggregates.
- **Currency-safe analytics:** Avoids summing monetary amounts across currencies.
- **Reconciliation:** Compares aggregate outputs against their source facts.
- **Orchestration and observability:** Uses Airflow retries and informative task logs.
- **Transparent business rules:** Distinguishes review indicators from confirmed fraud outcomes.
- **Version control:** Tracks implementation and documentation changes in Git.

## Future Improvements

Potential next steps include:

- Adding automated integration tests for the complete CDC-to-Gold flow.
- Documenting recovery, replay, and backfill procedures.
- Improving pipeline monitoring and alerting.
- Creating a dashboard for selected Gold metrics.
- Adding performance benchmarks and data-volume tests.
- Documenting cloud deployment options and operational trade-offs.

---

StreamGuard is a learning and portfolio project focused on building reliable data pipelines and producing trustworthy analytics-ready datasets using modern data engineering tools.
