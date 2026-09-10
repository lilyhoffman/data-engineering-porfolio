# Data Engineering Portfolio

A collection of data engineering projects focused on building reliable data pipelines using batch and real-time processing technologies.

These projects demonstrate hands-on experience with data ingestion, transformation, orchestration, distributed processing, data modeling, streaming, containerized infrastructure, and data quality.

## Projects

### 1. Financial Markets Data Platform

**Technologies:** Python, SQL, PySpark, Apache Airflow, dbt, PostgreSQL, Docker

Built an end-to-end batch data pipeline for financial market data using a Bronze/Silver/Gold architecture.

The pipeline incrementally ingests market data, transforms it with PySpark, loads processed data into PostgreSQL, and builds analytics-ready models with dbt. Apache Airflow orchestrates the complete workflow, including data quality testing, retries, and pipeline monitoring.

**Key Features:**
- Incremental data ingestion
- Bronze/Silver/Gold data architecture
- PySpark transformations
- PostgreSQL data warehouse
- dbt transformations and data quality tests
- Apache Airflow orchestration
- Pipeline audit logging and failure monitoring
- Dockerized infrastructure

[View Project](./financial-market-data-platform)

---

### 2. Real-Time E-Commerce Streaming Pipeline

**Technologies:** Python, Apache Kafka, PySpark, Spark Structured Streaming, PostgreSQL, Docker

Built a real-time streaming pipeline that generates simulated e-commerce orders, streams events through Apache Kafka, processes them with PySpark Structured Streaming, and stores validated orders and real-time metrics in PostgreSQL.

The pipeline implements event-time processing, windowed aggregations, watermarking, idempotent database writes, and checkpoint-based recovery.

**Key Features:**
- Real-time Kafka event ingestion
- PySpark Structured Streaming
- Schema validation and data transformation
- 5-minute event-time aggregations
- Watermarking for late-arriving events
- Idempotent PostgreSQL writes
- Spark checkpointing and restart recovery
- Dockerized infrastructure

[View Project](./real-time-ecommerce-pipeline)

---

## Technical Skills Demonstrated

**Languages:** Python, SQL

**Data Engineering:** Apache Spark, PySpark, Apache Kafka, Apache Airflow, dbt

**Databases:** PostgreSQL

**Infrastructure:** Docker, Docker Compose

**Data Engineering Concepts:**
- ETL / ELT Pipelines
- Batch Processing
- Stream Processing
- Incremental Data Ingestion
- Data Modeling
- Data Quality Testing
- Pipeline Orchestration
- Event-Time Processing
- Watermarking
- Idempotent Data Loads
- Checkpointing & Fault Recovery
- Bronze / Silver / Gold Architecture

## Repository Structure

```text
data-engineering-portfolio/
│
├── financial-market-data-platform/
│   ├── airflow/
│   ├── dbt/
│   ├── scripts/
│   ├── sql/
│   └── README.md
│
├── real-time-ecommerce-pipeline/
│   ├── producer/
│   ├── streaming/
│   ├── sql/
│   ├── docker/
│   └── README.md
│
└── README.md
