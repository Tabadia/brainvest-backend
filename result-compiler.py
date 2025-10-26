import json
import boto3

s3_client = boto3.client('s3')

def lambda_handler(event, context):

    print(event);

    bucket_name = "hidden-for-github"
    uniqueIdentifier = event.get("uniqueIdentifier")

    prefix = f"results/{uniqueIdentifier}/"

    required_files = [
        "location_results.json",
        "momentum_results.json",
        "sector_results.json",
        "size_results.json",
        "volatility_results.json"
    ]
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

    if "Contents" not in response:
        raise Exception(f"No files found under {prefix}")

    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    if "Contents" not in response:
        raise Exception(f"No files found under {prefix}")

    found_files = [obj["Key"].replace(prefix, "") for obj in response["Contents"]]
    missing_files = [f for f in required_files if f not in found_files]
    if missing_files:
        raise Exception(f"Missing required result files: {missing_files}")

    combined_data = {}

    for filename in required_files:
        key = f"{prefix}{filename}"
        try:
            s3_obj = s3_client.get_object(Bucket=bucket_name, Key=key)
            content = s3_obj["Body"].read().decode("utf-8")
            combined_data[filename.replace(".json", "")] = json.loads(content)
        except ClientError as e:
            raise Exception(f"Error reading {key}: {str(e)}")

    combined_key = f"{uniqueIdentifier}_combined_results.json"
    try:
        s3_client.put_object(
            Bucket=bucket_name,
            Key=combined_key,
            Body=json.dumps(combined_data, indent=4),
            ContentType="application/json"
        )
        print(f"Combined JSON saved to {combined_key}")
    except ClientError as e:
        raise Exception(f"Error uploading combined JSON: {str(e)}")    
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
