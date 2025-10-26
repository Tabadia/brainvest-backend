import json
import requests
import os

def lambda_handler(event, context):
    vercel_url = "hidden-for-github"
    
    try:
        response = requests.post(vercel_url, json=event, timeout=60)
        response.raise_for_status()
        return {
            "statusCode": 200,
            "body": response.text
        }
    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }
