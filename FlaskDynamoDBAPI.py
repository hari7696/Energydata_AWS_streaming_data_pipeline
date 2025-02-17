from flask import Flask
import boto3
from flask import request

app = Flask(__name__)
dynamodb = boto3.client("dynamodb")


@app.route("/health", methods=["GET"])
def health_check():
    return {"status": "healthy"}, 200


@app.route("/fetchrecords", methods=["GET"])
def fetch_records():
    """
    Fetch records from DynamoDB for a given site_id and date range.
    Query parameters:
    - site_id (str): The ID of the site to fetch records for.
    - start_date (str): The start date of the range to fetch records for.
    - end_date (str): The end date of the range to fetch records for.
    Returns:
    - list: A list of cleaned records from DynamoDB.
    """
    # fetch for given site_id and date and time range
    site_id = request.args.get("site_id")
    start_date = request.args.get("start_date")
    end_date = request.args.get("end_date")

    # Querying DynamoDB for records
    response = dynamodb.query(
        TableName="EnergyData",
        KeyConditionExpression="site_id = :site_id AND #ts BETWEEN :start AND :end",
        ExpressionAttributeNames={
            "#ts": "timestamp"  # Use an expression attribute name to avoid reserved word conflicts
        },
        ExpressionAttributeValues={
            ":site_id": {"S": site_id},
            ":start": {"S": start_date},
            ":end": {"S": end_date},
        },
    )

    # cleaning up the response
    items = response["Items"]
    cleaned_items = []
    for item in items:
        for key, value in item.items():
            item[key] = list(value.values())[0]

        cleaned_items.append(item)

    return cleaned_items


@app.route("/fetchanomalies", methods=["GET"])
def fetch_anomalies():
    """
    Fetches all anomalies for a given site_id from the DynamoDB table "EnergyData".
    This endpoint expects a GET request with a query parameter "site_id".
    It queries the DynamoDB table for items with the specified site_id and filters out the anomalies.
    Returns:
        list: A list of cleaned items where each item is a dictionary representing an anomaly.
    """

    # fetch all anomalies for given site_id
    site_id = request.args.get("site_id")

    # Querying DynamoDB for anomalies
    response = dynamodb.query(
        TableName="EnergyData",
        ExpressionAttributeValues={
            ":site_id": {"S": site_id},  # Example site ID
        },
        KeyConditionExpression="site_id = :site_id",
    )

    # cleaning up the response
    items = response["Items"]
    cleaned_items = []
    for item in items:
        for key, value in item.items():
            item[key] = list(value.values())[0]
        # since GSI doesnt support boolean, I am resorting to filtering the anomalies here. I awate its a bad practice and cost ineffective
        if item["anomaly"] == True:
            cleaned_items.append(item)

    return cleaned_items


@app.route("/fetchall", methods=["GET"])
def fetch_all():
    """
    Fetch all records from the DynamoDB table "EnergyData".
    This endpoint scans the DynamoDB table "EnergyData" and retrieves all records.
    The response is then cleaned up to extract the values from the DynamoDB data types.
    Returns:
        list: A list of dictionaries containing the cleaned records from the DynamoDB table.
    """

    # fetch for given site_id and date and time range
    # scan the table to fetch all records
    response = dynamodb.scan(TableName="EnergyData")
    items = response["Items"]

    # cleaning up the response
    cleaned_items = []
    for item in items:
        for key, value in item.items():
            item[key] = list(value.values())[0]

        cleaned_items.append(item)

    return cleaned_items


if __name__ == "__main__":
    app.run(debug=True)

# sample query url : http://127.0.0.1:5000/fetchall
# http://127.0.0.1:5000/fetchrecords?site_id=site1&start_date=2025-02-17T04:10:43Z&end_date=2025-02-17T04:20:43Z
# http://127.0.0.1:5000/fetchall?site_id=site1
