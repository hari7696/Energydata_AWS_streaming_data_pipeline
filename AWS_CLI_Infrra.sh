# creating the s3 bucket
aws s3 mb s3://energy_data2025 --region us-east-1

# create a lambda role
aws iam create-role --role-name lambda-exec-role --assume-role-policy-document file://policies/lambda_role.json

# creating a policy for s3 write access 
aws iam create-policy --policy-name LambdaS3SNSdynamodb --policy-document file://policies/s3_sns_dynamodb_accesspolicy.json

# Lambda function should have access to write to s3, read from s3, write to dynamo db, read from dynamo db and publish to sns
aws iam attach-role-policy --role-name lambda-exec-role --policy-arn arn:aws:iam::209089076730:policy/LambdaS3SNSdynamodb

# attaching basic execution policy to the lambda execution role for cloudwatch logs
aws iam attach-role-policy --role-name lambda-exec-role --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole

# creating a lambda function
aws lambda create-function --function-name DataGenerator \
    --zip-file fileb://lambda_zips/DataGenerator.zip \
    --handler lambda_function.lambda_handler \
    --runtime python3.12 \
    --role arn:aws:iam::209089076730:role/lambda-exec-role\
    --timeout 120

# creating eventbridge role
aws iam create-role --role-name EventBridgeSchedulerRole --assume-role-policy-document file://policies/eventbridge_policy.json
#"arn:aws:iam::209089076730:role/EventBridgeSchedulerRole"
# ataching the lambda invoke policy to the eventbridge role
aws iam put-role-policy --role-name EventBridgeSchedulerRole --policy-name InvokeLambdaPolicy --policy-document file://policies/lambda_invoke_policy.json

# a eventbridge rule to trigger the lambda function every 5 minutes, that inturn writes data to s3
aws scheduler create-schedule \
    --name DataGeneratorScheduler \
    --schedule-expression "rate(5 minutes)" \
    --flexible-time-window "Mode=OFF" \
    --target "Arn=arn:aws:lambda:us-east-1:209089076730:function:DataGenerator,RoleArn=arn:aws:iam::209089076730:role/EventBridgeSchedulerRole"

#================================================================================================================================================================

# creating a queue for s3 events
aws sqs create-queue --queue-name S3EventQueue
# "QueueUrl": "https://sqs.us-east-1.amazonaws.com/209089076730/S3EventQueue"
#arn:aws:sqs:us-east-1:209089076730:S3EventQueue

# updating SQS queue access policy
aws sqs set-queue-attributes --queue-url arn:aws:sqs:us-east-1:209089076730:S3EventQueue --attributes file://policies/sqs_access_policy.json
# manullay copied sqs_access_policy to queue via console [command not working]

# creating a dead letter queue for s3eventsqueue
aws sqs create-queue --queue-name FailedEnergyEvents
aws sqs set-queue-attributes --queue-url https://sqs.us-east-1.amazonaws.com/209089076730/S3EventQueue --attributes file://policies/dead_letter_queue_config.json


# creating a s3 event trigger to push new events to the queue
aws s3api put-bucket-notification-configuration --bucket energydata2025 --notification-configuration file://policies/s3_notificatin_config.json

# updating the lambda execution role to allow it to receive messages from the queue
aws iam attach-role-policy --role-name lambda-exec-role --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaSQSQueueExecutionRole

#creating a second lambda function to process the s3 events.
# This Lambda function will be triggered by the SQS queue and reads the event related data from the s3 bucket and writes to dynamo db
# also checks for anomalies and publishes to sns
aws lambda create-function --function-name processData \
    --zip-file fileb:////lambda_zips/processData.zip \
    --handler lambda_function.lambda_handler \
    --runtime python3.12 \
    --role arn:aws:iam::209089076730:role/lambda-exec-role \
    --timeout 120

# linking SQS queue to the lambda function
# Enabled partial sucess in batching via console, as I didn't find a way to do it via CLI
# https://docs.aws.amazon.com/lambda/latest/dg/services-sqs-errorhandling.html#services-sqs-batchfailurereporting
aws lambda create-event-source-mapping
    --function-name processData
    --batch-size 3
    --maximum-batching-window-in-seconds 5
    --event-source-arn arn:aws:sqs:us-east-1:209089076730:S3EventQueue

#dynamo DB table creation
aws dynamodb create-table \
    --table-name EnergyData \
    --attribute-definitions \
        AttributeName=site_id,AttributeType=S \
        AttributeName=timestamp,AttributeType=S \
    --key-schema \
        AttributeName=site_id,KeyType=HASH \
        AttributeName=timestamp,KeyType=RANGE \
    --provisioned-throughput \
        ReadCapacityUnits=5 \
        WriteCapacityUnits=5 \
    --region us-east-1
# manuually added a resource based policy to the dynamo db table dynamodb_resource_policy.json

# Creating a SNS topic for anomalies notification
aws sns create-topic --name EnergyAnomalies
aws sns subscribe --topic-arn "arn:aws:sns:us-east-1:209089076730:EnergyAnomalies" \
    --protocol email --notification-endpoint "hari.golamari2@gmail.com"
aws sns set-topic-attributes --topic-arn "arn:aws:sns:us-east-1:209089076730:EnergyAnomalies" \
    --attribute-name Policy \
    --attribute-value file://policies/sns_policy.json



