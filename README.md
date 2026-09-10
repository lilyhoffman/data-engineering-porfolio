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

### 3. Cloud Analytics Data Platform

**Technologies:** Python, SQL, PySpark, AWS S3, AWS Glue, AWS Glue Data Catalog, Amazon Athena, Terraform, Parquet

Built a cloud-based analytics data platform on AWS that ingests 3.48M NYC Yellow Taxi records into an S3 data lake and transforms raw data into cleaned, partitioned datasets using AWS Glue and PySpark.

The pipeline applies data quality rules, organizes analytics-ready Parquet data using time-based partitioning, catalogs the dataset with AWS Glue Data Catalog, and enables serverless SQL analytics through Amazon Athena. Terraform definitions provide Infrastructure as Code for the core AWS resources.

**Key Features:**

- Python-based ingestion into Amazon S3
- Bronze/Silver data lake architecture
- AWS Glue and PySpark ETL
- Data quality validation, removing 151K+ invalid records
- Partitioned Parquet storage
- AWS Glue Data Catalog integration
- Partition-aware Amazon Athena queries
- Infrastructure as Code with Terraform
- IAM-based AWS access control

[View Project](./cloud-analytics-data-platform)

---

## Technical Skills Demonstrated

**Languages:** Python, SQL

**Data Engineering:** Apache Spark, PySpark, Apache Kafka, Apache Airflow, dbt, AWS Glue

**Cloud & Analytics:** AWS S3, AWS Glue Data Catalog, Amazon Athena

**Databases:** PostgreSQL

**Infrastructure:** Docker, Docker Compose, Terraform

**Data Engineering Concepts:**

- ETL / ELT Pipelines
- Batch & Stream Processing
- Cloud Data Lakes
- Incremental Data Ingestion
- Distributed Data Processing
- Data Modeling
- Data Quality Testing
- Pipeline Orchestration
- Event-Time Processing & Watermarking
- Idempotent Data Loads
- Checkpointing & Fault Recovery
- Data Partitioning & Parquet Storage
- Infrastructure as Code
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
├── cloud-analytics-data-platform/
│   ├── ingestion/
│   ├── glue/
│   ├── sql/
│   ├── terraform/
│   └── README.md
│
└── README.md
```

Each project contains its own README with architecture, implementation details, setup instructions, and technical decisions.

## About

I am a data professional with experience in data analytics, automation, and backend development, with a particular interest in building reliable data systems and pipelines.

This portfolio focuses on applying data engineering technologies to practical projects involving batch processing, real-time streaming, distributed computing, orchestration, databases, and containerized infrastructure.

# Contact
[Github](https://github.com/lilyhoffman/data-engineering-porfolio) | [Linkedin](https://www.linkedin.com/in/lily-hoffman-79387425a/)
