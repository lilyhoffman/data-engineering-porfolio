# Cloud Analytics Data Platform

A cloud-based data engineering pipeline that ingests NYC Yellow Taxi trip data into an Amazon S3 data lake, transforms and validates the data with AWS Glue and PySpark, catalogs the resulting dataset with AWS Glue Data Catalog, and queries it using Amazon Athena.

The pipeline processes approximately 3.48 million raw trip records and produces a cleaned, partitioned Silver dataset containing approximately 3.32 million records.

## Architecture

```text
NYC TLC Trip Data
        |
        v
Python Ingestion
        |
        v
Amazon S3
Bronze Layer
        |
        v
AWS Glue + PySpark
        |
        |-- Data quality validation
        |-- Schema standardization
        |-- Derived fields
        |-- Partitioning
        v
Amazon S3
Silver Layer (Parquet)
        |
        v
AWS Glue Data Catalog
        |
        v
Amazon Athena
        |
        v
SQL Analytics
```

## Technologies

- Python
- PySpark
- Amazon S3
- AWS Glue
- AWS Glue Data Catalog
- Amazon Athena
- Terraform
- Parquet
- SQL
- boto3

## Project Structure

```text
cloud-analytics-data-platform/
├── ingestion/
│   └── ingest_taxi_data.py
├── glue/
│   └── transform_taxi_data.py
├── sql/
│   └── analytics.sql
├── terraform/
│   ├── main.tf
│   ├── variables.tf
│   ├── outputs.tf
│   └── terraform.tfvars.example
├── .gitignore
├── requirements.txt
└── README.md
```

## Data Source

The project uses January 2025 NYC Yellow Taxi Trip Record Data published by the New York City Taxi & Limousine Commission.

The source Parquet file contains 3,475,226 records.

## Bronze Layer

`ingestion/ingest_taxi_data.py` downloads the source Parquet dataset and uploads it to Amazon S3.

The Bronze layer preserves the source data before transformation.

```text
bronze/
└── yellow_taxi/
    └── year=2025/
        └── month=01/
            └── yellow_tripdata_2025-01.parquet
```

## Silver Transformation

AWS Glue runs a PySpark transformation job over the Bronze dataset.

The transformation:

- Standardizes column names
- Removes records with missing pickup or dropoff timestamps
- Removes trips with non-positive distance or total amount
- Removes records where dropoff occurs before pickup
- Calculates trip duration
- Removes trips with invalid or unrealistic durations
- Derives pickup year, month, and day
- Writes analytics-ready Parquet data partitioned by pickup year and month

After validation, the Silver dataset contains **3,324,165 records**, meaning **151,061 records (4.35%)** were removed by the data-quality rules.

The actual pickup timestamps produced three partitions:

```text
pickup_year=2024/pickup_month=12     21 records
pickup_year=2025/pickup_month=1      3,324,143 records
pickup_year=2025/pickup_month=2      1 record
```

This ensures records are partitioned according to their actual event timestamps rather than simply according to the source filename.

## Data Catalog

An AWS Glue crawler scans the Silver S3 layer and registers the dataset in the Glue Data Catalog.

The catalog database is:

```text
nyc_taxi_analytics
```

The resulting `yellow_taxi` table can be queried directly from Amazon Athena.

## Athena Analytics

`sql/analytics.sql` contains analytical queries for:

- Dataset-level KPIs
- Daily trip and revenue trends
- Payment method performance
- Pickup and dropoff location activity
- Trip distance analysis
- Tip analysis
- Airport trip analysis

Queries filter on the `pickup_year` and `pickup_month` partition columns so Athena can limit scans to the relevant S3 partitions.

For January 2025, the cleaned dataset contains:

| Metric | Result |
|---|---:|
| Trips | 3,324,143 |
| Total Revenue | $88.39M |
| Average Trip Value | $26.59 |
| Average Trip Distance | 6.03 miles |
| Average Trip Duration | 15.12 minutes |

## Infrastructure as Code

Terraform definitions are included for the major AWS infrastructure components:

- S3 data lake
- Glue Data Catalog database
- Glue IAM role and policies
- Glue ETL job
- Glue crawler
- Athena workgroup and query-result location

The Terraform configuration was formatted, initialized, and successfully validated.

The live AWS resources used for the project were created manually before the Terraform configuration was added, so Terraform was not applied against those existing resources.

## Data Quality

The Silver transformation enforces several validation rules before records become available for analytics:

```text
pickup_datetime IS NOT NULL
dropoff_datetime IS NOT NULL
trip_distance > 0
total_amount > 0
dropoff_datetime > pickup_datetime
trip_duration_minutes > 0
trip_duration_minutes <= 1440
```

These checks removed approximately **4.35%** of the original records.

## Key Engineering Concepts Demonstrated

- Cloud data lake architecture
- Bronze/Silver data organization
- Python-based ingestion
- S3 object storage
- Distributed ETL with PySpark
- Data quality validation
- Columnar Parquet storage
- Time-based partitioning
- AWS Glue Data Catalog
- Serverless SQL analytics with Athena
- Partition-aware querying
- IAM-based access control
- Infrastructure as Code with Terraform