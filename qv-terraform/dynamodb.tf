resource "aws_dynamodb_table" "qv_prices" {
  name         = "qv_prices_${var.env_name}"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "id"
  range_key    = "area"
  write_capacity = 0
  read_capacity = 0

  attribute {
    name = "id"
    type = "S"
  }

  attribute {
    name = "area"
    type = "S"
  }

  tags {
    Name        = "qv-prices"
    Environment = "${var.env_name}"
  }
}

resource "aws_dynamodb_table" "qv_blocks" {
  name         = "qv_blocks_${var.env_name}"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "id"
  write_capacity = 0
  read_capacity = 0

  attribute = {
    name = "id"
    type = "S"
  }

  tags {
    Name        = "qv-blocks"
    Environment = "${var.env_name}"
  }
}

resource "aws_dynamodb_table" "qv_bldgs" {
  name         = "qv_bldgs_${var.env_name}"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "id"
  write_capacity = 0
  read_capacity = 0

  attribute {
    name = "id"
    type = "S"
  }

  tags {
    Name        = "qv-bldgs"
    Environment = "${var.env_name}"
  }
}

resource "aws_dynamodb_table" "qv_costbuilder" {
  name         = "qv_costbuilder_${var.env_name}"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "id"
  write_capacity = 0
  read_capacity = 0

  attribute {
    name = "id"
    type = "S"
  }

  tags {
    Name        = "qv-costbuilder"
    Environment = "${var.env_name}"
  }
}
