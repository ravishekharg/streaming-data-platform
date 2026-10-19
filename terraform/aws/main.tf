# AWS-managed version of the platform: MSK Serverless (Kafka) -> EMR Serverless (Spark) -> Iceberg on S3 with the
# Glue Data Catalog -> Athena / Trino. Validated in CI (terraform validate + tflint); apply it in your own account.

locals {
  name = var.name
  tags = merge({ project = "streaming-data-platform", managed-by = "terraform" }, var.tags)
}

data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

# ---------------- S3 lake bucket ----------------
resource "aws_s3_bucket" "lake" {
  bucket = "${local.name}-lake-${data.aws_caller_identity.current.account_id}"
  tags   = local.tags
}

resource "aws_s3_bucket_versioning" "lake" {
  bucket = aws_s3_bucket.lake.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "lake" {
  bucket = aws_s3_bucket.lake.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "aws:kms"
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_public_access_block" "lake" {
  bucket                  = aws_s3_bucket.lake.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "lake" {
  bucket = aws_s3_bucket.lake.id
  rule {
    id     = "expire-old-versions-and-tmp"
    status = "Enabled"
    filter {}
    noncurrent_version_expiration {
      noncurrent_days = 30
    }
    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }
}

# ---------------- Glue Data Catalog (Iceberg tables) ----------------
resource "aws_glue_catalog_database" "layer" {
  for_each     = toset(["bronze", "silver", "gold", "quarantine"])
  name         = "${replace(local.name, "-", "_")}_${each.key}"
  location_uri = "s3://${aws_s3_bucket.lake.bucket}/warehouse/${each.key}.db"
}

# ---------------- MSK Serverless (IAM auth) ----------------
resource "aws_security_group" "msk" {
  name        = "${local.name}-msk"
  description = "MSK Serverless: Kafka IAM listener from the Spark subnets"
  vpc_id      = var.vpc_id
  ingress {
    description = "Kafka IAM auth"
    from_port   = 9098
    to_port     = 9098
    protocol    = "tcp"
    cidr_blocks = var.client_cidrs
  }
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
  tags = local.tags
}

resource "aws_msk_serverless_cluster" "this" {
  cluster_name = "${local.name}-kafka"
  vpc_config {
    subnet_ids         = var.private_subnet_ids
    security_group_ids = [aws_security_group.msk.id]
  }
  client_authentication {
    sasl {
      iam {
        enabled = true
      }
    }
  }
  tags = local.tags
}

# ---------------- EMR Serverless (Spark) ----------------
resource "aws_security_group" "emr" {
  name        = "${local.name}-emr"
  description = "EMR Serverless Spark workers"
  vpc_id      = var.vpc_id
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
  tags = local.tags
}

resource "aws_emrserverless_application" "spark" {
  name          = "${local.name}-spark"
  release_label = var.emr_release
  type          = "spark"
  maximum_capacity {
    cpu    = "64 vCPU"
    memory = "256 GB"
  }
  auto_start_configuration {
    enabled = true
  }
  auto_stop_configuration {
    enabled              = true
    idle_timeout_minutes = 15
  }
  network_configuration {
    subnet_ids         = var.private_subnet_ids
    security_group_ids = [aws_security_group.emr.id]
  }
  tags = local.tags
}

resource "aws_cloudwatch_log_group" "spark" {
  name              = "/emr-serverless/${local.name}"
  retention_in_days = 30
  tags              = local.tags
}

# ---------------- IAM: job runtime role (least privilege) ----------------
data "aws_iam_policy_document" "emr_trust" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["emr-serverless.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "job" {
  name               = "${local.name}-spark-job"
  assume_role_policy = data.aws_iam_policy_document.emr_trust.json
  tags               = local.tags
}

data "aws_iam_policy_document" "job" {
  statement {
    sid       = "LakeObjects"
    actions   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload"]
    resources = ["${aws_s3_bucket.lake.arn}/*"]
  }
  statement {
    sid       = "LakeBucket"
    actions   = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = [aws_s3_bucket.lake.arn]
  }
  statement {
    sid = "GlueCatalog"
    actions = ["glue:GetDatabase", "glue:GetDatabases", "glue:GetTable", "glue:GetTables", "glue:CreateTable",
    "glue:UpdateTable", "glue:DeleteTable", "glue:GetPartitions", "glue:BatchGetPartition"]
    resources = concat(
      ["arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:catalog"],
      [for db in aws_glue_catalog_database.layer : db.arn],
      [for db in aws_glue_catalog_database.layer : "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:table/${db.name}/*"],
    )
  }
  statement {
    sid       = "KafkaConnect"
    actions   = ["kafka-cluster:Connect", "kafka-cluster:DescribeCluster"]
    resources = [aws_msk_serverless_cluster.this.arn]
  }
  statement {
    sid     = "KafkaReadTopic"
    actions = ["kafka-cluster:DescribeTopic", "kafka-cluster:ReadData", "kafka-cluster:DescribeGroup", "kafka-cluster:AlterGroup"]
    resources = [
      "arn:aws:kafka:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:topic/${aws_msk_serverless_cluster.this.cluster_name}/*",
      "arn:aws:kafka:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:group/${aws_msk_serverless_cluster.this.cluster_name}/*",
    ]
  }
  statement {
    sid       = "Logs"
    actions   = ["logs:CreateLogStream", "logs:PutLogEvents", "logs:DescribeLogStreams"]
    resources = ["${aws_cloudwatch_log_group.spark.arn}:*"]
  }
}

resource "aws_iam_role_policy" "job" {
  name   = "lake-glue-msk"
  role   = aws_iam_role.job.id
  policy = data.aws_iam_policy_document.job.json
}
