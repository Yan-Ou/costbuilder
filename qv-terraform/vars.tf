variable "env_name" {
  description = "Your environment name"
}

#VPC variables
variable "vpc_cidr" {
  description = "The CIDR block for VPC"
}

variable "vpc_tenancy" {
  description = "VPC tenancy type"
}

variable "pub_subnet_cidr" {
  description = "The public subnet cidr block"
}

variable "pub_az" {
  description = "The availability zone for public subet"
}

variable "pri_subnet_cidr" {
  description = "The private subnet cidr block"
}

variable "pri_az" {
  description = "The availability zone for private subnet"
}

#S3 variables
variable "bucket_name" {
  description = "S3 bucket name"
}

#Keypair variables
variable "sshkey_name" {
  description = "EC2 SSH key name"
}

#EC2 variables
variable "instance_type" {
  description = "EC2 instance type"
}

variable "ingress_cidr" {
  description = "Inbound IP range"
}

#Batch variables
variable "batch_instance_type" {
  description = "Batch job compute environment instance type"
}

variable "batch_max_vcpus" {
  description = "Batch compute environment max vcpu count"
}

variable "batch_min_vcpus" {
  description = "Batch compute environment min vcpu count"
}
