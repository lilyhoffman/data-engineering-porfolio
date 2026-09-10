output "s3_bucket_name" {
  description = "S3 data lake bucket"
  value       = aws_s3_bucket.data_lake.bucket
}

output "glue_database_name" {
  description = "Glue Data Catalog database"
  value       = aws_glue_catalog_database.analytics.name
}

output "glue_job_name" {
  description = "Glue ETL job"
  value       = aws_glue_job.bronze_to_silver.name
}

output "glue_crawler_name" {
  description = "Glue crawler"
  value       = aws_glue_crawler.silver_crawler.name
}

output "athena_workgroup_name" {
  description = "Athena workgroup"
  value       = aws_athena_workgroup.analytics.name
}