variable "aws_region" {
  description = "AWS region for the project"
  type        = string
  default     = "us-east-1"
}

variable "project_name" {
  description = "Project name used for AWS resource naming"
  type        = string
  default     = "nyc-taxi-data-platform"
}

variable "bucket_name" {
  description = "S3 bucket used for Bronze, Silver, and Athena query results"
  type        = string
}

variable "glue_database_name" {
  description = "AWS Glue Data Catalog database name"
  type        = string
  default     = "nyc_taxi_analytics"
}

variable "glue_job_name" {
  description = "AWS Glue ETL job name"
  type        = string
  default     = "nyc-taxi-bronze-to-silver"
}

variable "glue_crawler_name" {
  description = "AWS Glue crawler name"
  type        = string
  default     = "nyc-taxi-silver-crawler"
}