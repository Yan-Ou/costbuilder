provider "aws" {
  region = "ap-southeast-2"
}

terraform {
  backend "s3" {}
}
