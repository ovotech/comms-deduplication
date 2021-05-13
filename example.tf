# Configure the AWS Provider
provider "aws" {
  region = "eu-west-1"
}

# Table
resource "aws_dynamodb_table" "deduplication-table" {
  name           = "deduplication-table"
  hash_key       = "id"
  range_key      = "processorId"
  
  billing_mode   = "PAY_PER_REQUEST"

  point_in_time_recovery {
    enabled = false
  }

  lifecycle {
    prevent_destroy = true
  }
  
  attribute {
    name = "id"
    type = "S"
  }
  
  attribute {
    name = "processorId"
    type = "S"
  }

  ttl {
    attribute_name = "expiresOn"
    enabled        = true
  }
}