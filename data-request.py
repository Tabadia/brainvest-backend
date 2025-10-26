import json
import os
import logging
from datetime import datetime, timedelta
import boto3
from botocore.exceptions import ClientError
import urllib.parse
import requests

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_client = boto3.client('s3')

def call_vercel_function(bucket, key):
    try:
        vercel_url = "hidden-for-github"
        payload = {
            "bucket": bucket,
            "key": key
        }
        
        logger.info(f"Calling Vercel function: {vercel_url}")
        response = requests.post(
            vercel_url,
            json=payload,
            headers={'Content-Type': 'application/json'},
            timeout=300  # 5 mins!!
        )
        
        if response.status_code == 200:
            result = response.json()
            logger.info(f"Vercel function completed successfully: {result}")
            return result
        else:
            error_msg = f"Vercel function failed with status {response.status_code}: {response.text}"
            logger.error(error_msg)
            raise Exception(error_msg)
            
    except requests.exceptions.Timeout:
        error_msg = "Vercel function timed out"
        logger.error(error_msg)
        raise Exception(error_msg)
    except Exception as e:
        error_msg = f"Error calling Vercel function: {str(e)}"
        logger.error(error_msg)
        raise Exception(error_msg)



def lambda_handler(event, context):
    try:
        logger.info("Starting data request")
        
        bucket = None
        key = None
        
        if 'Records' in event and len(event['Records']) > 0:
            record = event['Records'][0]
            if record.get('eventSource') == 'aws:s3':
                bucket = record['s3']['bucket']['name']
                key = urllib.parse.unquote_plus(record['s3']['object']['key'])
                logger.info(f"Processing S3 object: s3://{bucket}/{key}")
            else:
                return {
                    'statusCode': 400,
                    'body': json.dumps({'error': 'Not an S3 event'})
                }
        else:
            if isinstance(event, str):
                event_data = json.loads(event)
            else:
                event_data = event
            
            bucket = event_data.get('bucket')
            key = event_data.get('key')
            
            if not bucket or not key:
                return {
                    'statusCode': 400,
                    'body': json.dumps({'error': 'Missing bucket or key in event'})
                }
        
        # Call vercel!!
        logger.info(f"Calling Vercel function for s3://{bucket}/{key}")
        vercel_result = call_vercel_function(bucket, key)
        
        logger.info("Portfolio enrichment orchestration completed successfully")
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'status': 'complete',
                'vercel_result': vercel_result,
                'message': 'Portfolio enrichment completed via Vercel function'
            }, default=str)
        }
        
    except Exception as e:
        logger.error(f"Lambda orchestration error: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }
