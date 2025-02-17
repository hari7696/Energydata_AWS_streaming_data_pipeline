import json
import boto3
from datetime import datetime, timezone
import random
import logging

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event, context):

    logger.info("Generating mockup data")
    data = generate_mockup_data()

    logger.info(f"Data generated with {len(data)} records. Uploading to S3.")
    upload_to_s3(data)

    logger.info("Data upload completed successfully.")


def generate_mockup_data():

    data = []
    # dummy sites
    sites = [
        "site1",
        "site2",
        "site3",
        "site4",
        "site5",
        "site6",
        "site7",
        "site8",
        "site9",
        "site10",
    ]
    timestamp = (
        datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
    )

    for site in sites:
        energy_generated_kwh = random.uniform(0, 100)
        energy_consumed_kwh = random.uniform(0, 100)

        # Introducing anomalies with 20% probability
        if random.random() < 0.2:
            energy_consumed_kwh = -1
        if random.random() < 0.2:
            energy_generated_kwh = -1

        data.append(
            {
                "site_id": site,
                "timestamp": timestamp,
                "energy_generated_kwh": round(energy_generated_kwh, 4),
                "energy_consumed_kwh": round(energy_consumed_kwh, 4),
            }
        )
    return data


def upload_to_s3(data):
    s3 = boto3.client("s3")
    bucket_name = "energydata2025"  # Ensure this bucket exists in your AWS account

    for item in data:
        site = item["site_id"]
        timestamp = (
            item["timestamp"].replace(":", "").replace("-", "")
        )  # Sanitize timestamp for S3 key
        file_name = f"{site}/{timestamp}.json"
        try:
            s3.put_object(
                Bucket=bucket_name, Key=file_name, Body=json.dumps(item, indent=4)
            )
        except Exception as e:
            logger.error(
                f"upload_to_s3: Error uploading {file_name} to S3 bucket {bucket_name}: {e}"
            )
            return
        logger.info(f"Uploaded {file_name} to S3 bucket {bucket_name}")
