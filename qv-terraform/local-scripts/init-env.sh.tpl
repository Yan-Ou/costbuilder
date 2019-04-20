# Script to switch local terraform context to remote backend S3
# Run in project root directory
#
# Purpose is to be able to run manual operations
# like 'terraform show' or 'terraform destroy' 

export AWS_ACCESS_KEY_ID=
export AWS_SECRET_ACCESS_KEY=

terraform init -backend-config="bucket=<state-bucket-name>" -backend-config="key=terraform/terraform.tfstate" -backend-config="region=ap-southeast-2"
