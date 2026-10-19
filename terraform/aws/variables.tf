variable "name" {
  description = "Name prefix for all resources"
  type        = string
  default     = "sdp"
  validation {
    condition     = can(regex("^[a-z][a-z0-9-]{2,20}$", var.name))
    error_message = "Use 3-21 lowercase letters, digits or hyphens."
  }
}

variable "vpc_id" {
  description = "VPC for MSK Serverless and EMR Serverless"
  type        = string
}

variable "private_subnet_ids" {
  description = "Private subnets (at least 2 AZs) for MSK and EMR Serverless"
  type        = list(string)
  validation {
    condition     = length(var.private_subnet_ids) >= 2
    error_message = "Provide at least two subnets in different AZs."
  }
}

variable "client_cidrs" {
  description = "CIDRs allowed to reach Kafka (the Spark / producer subnets)"
  type        = list(string)
}

variable "emr_release" {
  description = "EMR Serverless release (Spark 3.5)"
  type        = string
  default     = "emr-7.5.0"
}

variable "tags" {
  type    = map(string)
  default = {}
}
