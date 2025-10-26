import json
import boto3
import csv
import io
import urllib.parse
from datetime import datetime
from typing import Dict, Any

s3_client = boto3.client("s3")

DESTINATION_BUCKET = "hidden-for-github"

def lambda_handler(event, context):
    try:
        print("Event received:", json.dumps(event))

        record = event["Records"][0]
        source_bucket = record["s3"]["bucket"]["name"]
        source_key = urllib.parse.unquote_plus(record["s3"]["object"]["key"])

        print(f"Triggered by upload: s3://{source_bucket}/{source_key}")

        # Download CSV
        response = s3_client.get_object(Bucket=source_bucket, Key=source_key)
        csv_content = response["Body"].read().decode("utf-8")
        print(f"Downloaded CSV ({len(csv_content)} bytes)")

        # Parse CSV
        portfolio_data = parse_portfolio_csv(csv_content)
        print(f"Parsed {len(portfolio_data['holdings'])} holdings successfully")

        json_output = {
            "metadata": {
                "processed_at": datetime.utcnow().isoformat(),
                "source_file": f"s3://{source_bucket}/{source_key}",
                "total_holdings": len(portfolio_data["holdings"]),
                "account_value": portfolio_data["account_summary"].get("Net Account Value", 0),
            },
            "account_summary": portfolio_data["account_summary"],
            "holdings": portfolio_data["holdings"],
        }

        filename = source_key.split("/")[-1]
        identifier, original_filename = filename.split("-", 1)
        base_name = original_filename.rsplit(".", 1)[0]
        destination_key = f"csv-uploads/{identifier}/{base_name}.json"
            
        s3_client.put_object(
            Bucket=DESTINATION_BUCKET,
            Key=destination_key,
            Body=json.dumps(json_output, indent=2),
            ContentType="application/json",
        )

        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Portfolio data successfully converted to JSON",
                "source": f"s3://{source_bucket}/{source_key}",
                "destination": f"s3://{DESTINATION_BUCKET}/{destination_key}",
                "holdings_count": len(portfolio_data["holdings"]),
            }),
        }

    except Exception as e:
        print(f"Error processing file: {str(e)}")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": f"Failed to process portfolio data: {str(e)}"}),
        }


def parse_portfolio_csv(csv_content: str) -> Dict[str, Any]:

    csv_reader = csv.reader(io.StringIO(csv_content))
    rows = list(csv_reader)
    print(f"CSV has {len(rows)} rows")

    holdings_start = None
    account_summary = {}

    for i, row in enumerate(rows):
        if len(row) > 0 and row[0] == "Symbol" and "Day's Gain $" in row:
            holdings_start = i
            print(f"Found holdings header at row {i}")
        elif len(row) > 0 and "Account" in row[0] and "Net Account Value" in row:
            print(f"Found account summary header at row {i}")
            if i + 1 < len(rows):
                account_data = rows[i + 1]
                try:
                    account_summary = {
                        "Account": account_data[0].strip('"'),
                        "Net Account Value": float(account_data[1]) if account_data[1] else 0,
                        "Total Gain $": float(account_data[2]) if account_data[2] else 0,
                        "Total Gain %": float(account_data[3]) if account_data[3] else 0,
                        "Day's Gain Unrealized $": float(account_data[4]) if account_data[4] else 0,
                        "Day's Gain Unrealized %": float(account_data[5]) if account_data[5] else 0,
                        "Available For Withdrawal": float(account_data[6]) if account_data[6] else 0,
                        "Cash Purchasing Power": float(account_data[7]) if account_data[7] else 0,
                    }
                    print("Account summary parsed successfully")
                except ValueError:
                    print("Error parsing account summary row.")

    holdings = []
    if holdings_start is not None:
        for i in range(holdings_start + 1, len(rows)):
            row = rows[i]
            if len(row) >= 13 and row[0] not in ("TOTAL", "CASH", ""):
                try:
                    holdings.append({
                        "symbol": row[0],
                        "days_gain_dollar": float(row[1]) if row[1] else 0,
                        "days_gain_percent": float(row[2]) if row[2] else 0,
                        "quantity": float(row[3]) if row[3] else 0,
                        "total_gain_dollar": float(row[4]) if row[4] else 0,
                        "total_gain_percent": float(row[5]) if row[5] else 0,
                        "last_price": float(row[6]) if row[6] else 0,
                        "value": float(row[7]) if row[7] else 0,
                        "portfolio_percentage": float(row[8]) if row[8] else 0,
                        "dividend_yield": float(row[9]) if row[9] else 0,
                        "pe_ratio": float(row[10]) if row[10] else 0,
                        "eps": float(row[11]) if row[11] else 0,
                        "market_cap": row[12] if row[12] else "",
                        "beta": float(row[13]) if row[13] else 0,
                    })
                except (ValueError, IndexError):
                    print(f"Skipping malformed row {i}")
                    continue
    print(f"Extracted {len(holdings)} valid holdings")

    return {"account_summary": account_summary, "holdings": holdings}
