# CloudFormation templates for CI/CD pipelines

## Terraform (pipeline-terraform.yml)
This cf template creates a stack (parametrized by environment), containing an S3 bucket to manage the terraform statefile, as well as an AWS CodeBuild step to fetch the terraform templates from this git repo and apply it in the environment.

Create stack:

    aws cloudformation create-stack --stack-name CICD-<ENV> --capabilities CAPABILITY_NAMED_IAM --template-body file:///path/to/project/qv-terraform/pipeline/pipeline-terraform.yml --parameters ParameterKey=BitbucketUser,ParameterValue=qrious-qv-system ParameterKey=BitbucketPasswordParamStore,ParameterValue=/CloudBuild/bitbucket/password/qrious-qv-system ParameterKey=BitbucketRepoBranch,ParameterValue=<BRANCH> ParameterKey=Environment,ParameterValue=<ENV>

Make sure that the AWS Parameter Store contains the given entry to the bitbucket password (in the example '/CloudBuild/bitbucket/password/qrious-qv-system')

After successful stack creation, the codebuild job 'CICD-<ENV>-TerraformApplyStep' can be manually started in the AWS console to apply the terraform templates from this repository in its dedicated environment. All resources will tagged and named after the environment parameter defined earlier.

## Batch Job Image
This cf template creates a global stack (not environment specific), that contains a AWS CodeBuild step to build and push a docker image from the bitbucket repository 'qriousnz/qv_costbuilder.git'. The ECR repostiory is currently assumed to exist, but should also be created within this stack.

Create Stack:

    aws cloudformation create-stack --stack-name CICD-GLOBAL --capabilities CAPABILITY_NAMED_IAM --template-body file:///path/to/project/qv-terraform/pipeline/pipeline-batchimage.yml --parameters ParameterKey=BitbucketUser,ParameterValue=qrious-qv-system ParameterKey=BitbucketPasswordParamStore,ParameterValue=/CloudBuild/bitbucket/password/qrious-qv-system

Once the stack is created the codebuild job can be run via AWS console. Environment variables for docker tag and branch are set to 'latest' and 'master' by default, but can be overridden.
