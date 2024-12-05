#!/bin/bash
# shellcheck disable=SC2006

# Exit immediately if a command exits with a non-zero status
set -e

# Load environment variables from .env file
if [ -f .env ]; then
  source .env
else
  echo ".env file not found. Make sure it exists in the directory."
  exit 1
fi

# Ensure required environment variables are set
if [ -z "$STACK_NAME" ] || [ -z "$BUCKET_NAME" ] || [ -z "$DATA_FILE_PATH" ]; then
  echo "Required environment variables (STACK_NAME, BUCKET_NAME, DATA_FILE_PATH) not set in .env"
  exit 1
fi

# Print environment variables for debugging purposes
echo "AWS_ACCESS_KEY_ID: $AWS_ACCESS_KEY_ID"
echo "AWS_SECRET_ACCESS_KEY: $AWS_SECRET_ACCESS_KEY"
echo "STACK_NAME: $STACK_NAME"
echo "BUCKET_NAME: $BUCKET_NAME"
echo "DATA_FILE_PATH: $DATA_FILE_PATH"

# Set AWS credentials as environment variables
export AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID
export AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY

# Notify the user that the stack update process is starting
echo "Starting stack update process..."

# Define the CloudFormation template file name
template_file="cloudformation.yaml"

# Check if the stack exists by attempting to describe it
stack_exists=$(aws cloudformation describe-stacks --stack-name "$STACK_NAME" 2>&1 || echo -1)

# Function to deploy the stack
deploy_stack() {
  echo "Deploying stack: $STACK_NAME"
  aws cloudformation deploy \
    --template-file "$template_file" \
    --stack-name "$STACK_NAME" \
    --capabilities CAPABILITY_NAMED_IAM CAPABILITY_AUTO_EXPAND \
    --no-fail-on-empty-changeset
}

# If the stack does not exist (indicated by -1), create a new one
if echo "$stack_exists" | grep -q 'does not exist'; then
    echo "Creating a new stack: $STACK_NAME"
    deploy_stack
else
    # If the stack exists, update it
    echo "Updating the stack: $STACK_NAME"
    deploy_stack
fi

# Output the status of the operation
if [ $? -ne 0 ]; then
    echo "$STACK_NAME operation failed."
    exit 1
else
    echo "$STACK_NAME operation completed successfully."
fi

# Wait for the stack operation to complete
echo "Waiting for stack operation to complete..."
aws cloudformation wait stack-create-complete --stack-name "$STACK_NAME" || aws cloudformation wait stack-update-complete --stack-name "$STACK_NAME"

if [ $? -ne 0 ]; then
    echo "Stack operation did not complete successfully."
    exit 1
else
    echo "Stack operation completed successfully."
fi

# Upload the dataset to S3
echo "Uploading dataset to S3 bucket: $BUCKET_NAME"
aws s3 cp "$DATA_FILE_PATH" "s3://$BUCKET_NAME/"

# Confirm the data upload was successful
if [ $? -ne 0 ]; then
    echo "Data upload to S3 failed."
    exit 1
else
    echo "Data upload to S3 completed successfully."
fi

# Run the PySpark script
echo "Running the PySpark script"
spark-submit data_engineering_assignment.py

# Confirm the PySpark script execution was successful
if [ $? -ne 0 ]; then
    echo "PySpark script execution failed."
    exit 1
else
    echo "PySpark script execution completed successfully."
fi