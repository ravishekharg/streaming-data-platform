output "lake_bucket" {
  value = aws_s3_bucket.lake.bucket
}

output "glue_databases" {
  value = { for k, db in aws_glue_catalog_database.layer : k => db.name }
}

output "msk_cluster_arn" {
  value = aws_msk_serverless_cluster.this.arn
}

output "emr_application_id" {
  value = aws_emrserverless_application.spark.id
}

output "spark_job_role_arn" {
  value = aws_iam_role.job.arn
}
