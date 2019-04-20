resource "aws_vpc_endpoint" "s3" {
  vpc_id       = "${aws_vpc.qv.id}"
  service_name = "com.amazonaws.ap-southeast-2.s3"
}

resource "aws_s3_bucket" "qv" {
  bucket = "${var.bucket_name}-${var.env_name}"
  acl    = "private"

  server_side_encryption_configuration {
    rule {
      apply_server_side_encryption_by_default {
        sse_algorithm     = "aws:kms"
      }
    }
  }

  tags = {
    Name        = "QV data bucket"
    Environment = "${var.env_name}"
  }
}
