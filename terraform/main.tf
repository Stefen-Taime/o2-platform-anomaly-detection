# =============================================================================
# O2 Platform — Root Terraform Configuration
# =============================================================================

terraform {
  required_version = ">= 1.5.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.5"
    }
    snowflake = {
      source  = "Snowflake-Labs/snowflake"
      version = "~> 0.100"
    }
  }
}

# --- Providers ---
provider "aws" {
  region = var.region

  default_tags {
    tags = {
      Project     = var.project
      Environment = "poc"
      UseCase     = "10-coinbase-o2"
      ManagedBy   = "terraform"
    }
  }
}

provider "snowflake" {
  account  = var.snowflake_account
  user     = var.snowflake_user
  password = var.snowflake_password
  role     = var.snowflake_role
}

# =============================================================================
# Module Calls
# =============================================================================

# --- VPC + Networking + Security Groups ---
module "vpc" {
  source = "./modules/vpc"

  project  = var.project
  vpc_cidr = var.vpc_cidr
  my_ip    = var.my_ip
}

# --- S3 Buckets ---
module "s3" {
  source = "./modules/s3"

  project = var.project
}

# --- DynamoDB Tables ---
module "dynamodb" {
  source = "./modules/dynamodb"

  project = var.project
}

# --- SNS + Lambda (Slack Alerting) ---
module "sns_lambda" {
  source = "./modules/sns-lambda"

  project           = var.project
  slack_webhook_url = var.slack_webhook_url
}

# --- EKS Cluster + Node Group + IAM + ECR ---
module "eks" {
  source = "./modules/eks"

  project                = var.project
  public_subnet_ids      = module.vpc.public_subnet_ids
  private_subnet_ids     = module.vpc.private_subnet_ids
  eks_additional_sg_id   = module.vpc.eks_additional_sg_id
  eks_node_instance_type = var.eks_node_instance_type
  eks_node_count         = var.eks_node_count

  dynamodb_table_arns = [
    module.dynamodb.feature_store_arn,
    module.dynamodb.anomaly_scores_arn
  ]

  s3_bucket_arns = [
    module.s3.audit_trail_bucket_arn,
    "${module.s3.audit_trail_bucket_arn}/*",
    module.s3.artifacts_bucket_arn,
    "${module.s3.artifacts_bucket_arn}/*"
  ]

  sns_topic_arn = module.sns_lambda.sns_topic_arn
}

# --- EC2 Instances (MLflow + Jenkins) ---
module "ec2" {
  source = "./modules/ec2"

  project               = var.project
  region                = var.region
  mlflow_instance_type  = var.mlflow_instance_type
  jenkins_instance_type = var.jenkins_instance_type
  public_subnet_ids     = module.vpc.public_subnet_ids
  mlflow_sg_id          = module.vpc.mlflow_sg_id
  jenkins_sg_id         = module.vpc.jenkins_sg_id
  artifacts_bucket_id   = module.s3.artifacts_bucket_id
  artifacts_bucket_arn  = module.s3.artifacts_bucket_arn
}

# --- Snowflake (Database, Schemas, Tables, Snowpipe) ---
module "snowflake" {
  source = "./modules/snowflake"

  audit_trail_bucket_id     = module.s3.audit_trail_bucket_id
  snowflake_storage_role_arn = var.snowflake_storage_role_arn
}
