import logging
import boto3
import json

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def process_data(record, dynamodb):
    # Convert the SQS message body to a dictionary
    body = json.loads(record["body"])
    bucket = body["Records"][0]["s3"]["bucket"]["name"]
    key = body["Records"][0]["s3"]["object"]["key"]
    logger.info(f"Processing S3 object: {bucket}/{key}")

    # Create S3 client
    s3 = boto3.client("s3")
    logger.info(f"S3 client created")

    # Read the data from the S3 bucket
    response = s3.get_object(Bucket=bucket, Key=key)
    data = json.loads(response["Body"].read().decode("utf-8"))
    logger.info(f"Data retrieved from S3 object: {data}")

    try:
        anomaly = False
        # Check for anomalies in the data
        if (
            float(data["energy_generated_kwh"]) < 0
            or float(data["energy_consumed_kwh"]) < 0
        ):
            anomaly = True

        # Write the data to the DynamoDB table
        dynamodb.put_item(
            TableName="EnergyData",
            Item={
                "site_id": {"S": data["site_id"]},
                "timestamp": {"S": data["timestamp"]},
                "energy_generated_kwh": {"N": str(data["energy_generated_kwh"])},
                "energy_consumed_kwh": {"N": str(data["energy_consumed_kwh"])},
                "net_energy_kwh": {
                    "N": str(data["energy_generated_kwh"] - data["energy_consumed_kwh"])
                },
                "anomaly": {"BOOL": anomaly},
            },
        )

    except Exception as e:
        # Log and raise an error if data processing fails
        logger.error(f"Error processing data for bucket {bucket} and key {key}: {e}")
        raise ValueError(f"Error processing data: {e}")

    try:
        # Send SNS notification if anomaly detected
        if anomaly:
            sns = boto3.client("sns")
            sns.publish(
                TopicArn="arn:aws:sns:us-east-1:209089076730:EnergyAnomalies",
                Message=f"Anomaly detected in site: {data['site_id']} at {data['timestamp']}",
                Subject="Anomaly Detected",
            )
    except Exception as e:
        # Log an error if SNS notification fails
        logger.error(f"Error sending SNS notification On Anomaly detection: {e}")


def lambda_handler(event, context):
    logger.info(f"Received event: {event}")
    if event:
        batch_item_failures = []
        sqs_batch_response = {}

        # Create DynamoDB client outside the loop to avoid creating multiple clients
        dynamodb = boto3.client("dynamodb")
        logger.info(f"Event: {type(event)}")
        for record in event["Records"]:
            process_data(record, dynamodb)
        # Return batch item failures to the Lambda function
        sqs_batch_response["batchItemFailures"] = batch_item_failures
        return sqs_batch_response


# Uncomment the following block to test the function locally
# if __name__ == '__main__':
#     sample = {"Records": [{"messageId": "c324e11b-5cfd-4d2f-a2d8-c1e616b39e12",
# "receiptHandle": "AQEBU/9lrpdNkEWie5m6mANxDGGFxkGdh2rveydbxk4Kz3rfx13P+BUx6voKoVJyn8eJ6zCgFwIkMqNoeo0wcx9xPiAYYBWR1t5+Bmd1o6KqcKJMxQcTOtz+FrJ0ELEAavgFMtl45rxmp+hQGdWAMFFPyFOusafoKL28EW7KO6l9VJfbUT8MHQe5gsnI3qVQP9mIPxhHxS7D/XR6QXOuf/lW0zvXvw7PvVqFAKnyxboLc7lCVMxzf3v7Xo3I578JB793QAxhckKtQ+xl77FOYSCVUmpZn6CiT3illGXqXKGD/TMQ+oo233BY4xWGh7PT77aNSy7Mksv7V5e0d9D+CbkTd8xKKLii6R0PYqGzluofqsoPPfYCKGObwFpbXY/lGKqGVfgL5XQG2TSbGMupsw3nxA==",
# "body": {"Records":[{"eventVersion":"2.1","eventSource":"aws:s3","awsRegion":"us-east-1","eventTime":"2025-02-16T22:20:44.324Z","eventName":"ObjectCreated:Put","userIdentity":{"principalId":"AWS:AROATBLVODX5FCFUVYMBT:DataGenerator"},"requestParameters":{"sourceIPAddress":"34.229.210.105"},"responseElements":{"x-amz-request-id":"RDX8Q546TQXM4S6Z","x-amz-id-2":"R9/e7MdgC433R8kDe3HP+XzsoIQfYNcB+AurMEkwTDOqHpPhVndspt6Y9lNxXGeacjOhOhBfdaD9nD8+NbSjUT5MXM0+AAKH"},"s3":{"s3SchemaVersion":"1.0","configurationId":"QueueConfig","bucket":{"name":"energydata2025","ownerIdentity":{"principalId":"A3BPAB04KAA37L"},"arn":"arn:aws:s3:::energydata2025"},"object":{"key":"site1/20250216T213043.757740+0000.json","size":142,"eTag":"149d2739b67f97ea59534b4be1bd9878","sequencer":"0067B264BC4794BFE5"}}}]}
# }]}
#     lambda_handler(sample, None)
