import json
import boto3
import math
from typing import Dict, Any, List
from datetime import datetime

sp500_sectors = {
    "Information Technology": 27.5,
    "Health Care": 13.5,
    "Financials": 11.0,
    "Consumer Discretionary": 10.0,
    "Communication Services": 9.0,
    "Industrials": 8.5,
    "Consumer Staples": 7.0,
    "Energy": 4.0,
    "Utilities": 3.5,
    "Real Estate": 3.5,
    "Materials": 2.5
}

def normalize_sector_allocations(sectors: Dict[str, float]) -> Dict[str, float]:
    total = sum(sectors.values())
    if total == 0:
        return sectors
    
    return {sector: (percentage / total) * 100 for sector, percentage in sectors.items()}

def calculate_similarity(sp500: Dict[str, float], user: Dict[str, float]) -> float:
    print("Calculating similarity between SP500 and user sector allocations")
    sp500_normalized = normalize_sector_allocations(sp500)
    user_normalized = normalize_sector_allocations(user)
    
    all_sectors = set(sp500_normalized.keys()) | set(user_normalized.keys())
    
    sp500_vector = [sp500_normalized.get(sector, 0.0) for sector in all_sectors]
    user_vector = [user_normalized.get(sector, 0.0) for sector in all_sectors]
    
    dot_product = sum(a * b for a, b in zip(sp500_vector, user_vector))
    magnitude_sp500 = math.sqrt(sum(a * a for a in sp500_vector))
    magnitude_user = math.sqrt(sum(b * b for b in user_vector))
    
    if magnitude_sp500 == 0 or magnitude_user == 0:
        return 0.0
    
    cosine_similarity = dot_product / (magnitude_sp500 * magnitude_user)
    similarity_percentage = round(cosine_similarity * 100, 2)
    print(f"Similarity calculation complete: {similarity_percentage}%")
    return similarity_percentage

def get_bias_analysis(sp500: Dict[str, float], user: Dict[str, float], similarity: float) -> str:
    print(f"Getting bias analysis from Bedrock for similarity: {similarity}%")
    bedrock = boto3.client('bedrock-runtime', region_name='us-east-1')
    
    prompt = f"""
    Analyze the following sector allocation data and provide a brief, easy-to-understand insight about this portfolio's investment approach:
    
    SP500 Sector Allocation: {json.dumps(sp500, indent=2)}
    Portfolio Sector Allocation: {json.dumps(user, indent=2)}
    Similarity Score: {similarity}%
    
    IMPORTANT: Similarity interpretation:
    - 0-20%: Very different from market (concentrated/specialized approach)
    - 20-50%: Moderate deviation (some sector tilts)
    - 50-80%: Reasonably aligned with market
    - 80-100%: Very close to market allocation
    
    Please provide a brief one-sentence analysis explaining what this portfolio's sector allocation means in simple terms.
    """
    
    try:
        response = bedrock.invoke_model(
            modelId='anthropic.claude-3-sonnet-20240229-v1:0',
            body=json.dumps({
                "anthropic_version": "bedrock-2023-05-31",
                "max_tokens": 200,
                "messages": [
                    {
                        "role": "user",
                        "content": prompt
                    }
                ]
            }),
            contentType='application/json'
        )
        
        response_body = json.loads(response['body'].read())
        analysis = response_body['content'][0]['text']
        print("Successfully received bias analysis from Bedrock")
        return analysis
    
    except Exception as e:
        print(f"Error getting analysis from Bedrock: {str(e)}")
        raise Exception(f"Failed to get analysis from Bedrock: {str(e)}")

def process_holdings_to_sectors(holdings: List[Dict[str, Any]]) -> Dict[str, float]:
    print(f"Processing {len(holdings)} holdings for sector analysis")
    sector_allocations = {}
    
    for holding in holdings:
        sector = holding.get('sector', 'Unknown')
        percentage = holding.get('portfolio_percentage', 0.0)
        
        if sector in sector_allocations:
            sector_allocations[sector] += percentage
        else:
            sector_allocations[sector] = percentage
    
    print(f"Sector analysis complete. Found {len(sector_allocations)} unique sectors")
    return sector_allocations

def save_to_s3(bucket_name: str, key: str, data: Dict[str, Any]) -> bool:
    try:
        print(f"Attempting to save to S3 bucket: {bucket_name}, key: {key}")
        s3 = boto3.client('s3')
        
        try:
            s3.head_bucket(Bucket=bucket_name)
            print(f"S3 bucket '{bucket_name}' exists and is accessible")
        except Exception as bucket_error:
            print(f"Error accessing S3 bucket '{bucket_name}': {str(bucket_error)}")
            return False
        
        s3.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=json.dumps(data, indent=2),
            ContentType='application/json'
        )
        print(f"Successfully saved to S3: s3://{bucket_name}/{key}")
        return True
    except Exception as e:
        print(f"Error saving to S3: {str(e)}")
        print(f"Error type: {type(e).__name__}")
        return False

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    print("Starting sector analysis lambda handler")
    try:
        unique_identifier = event.get('uniqueIdentifier')
        data = event.get('data', {})
        print(f"Processing request for unique identifier: {unique_identifier}")
        
        if not unique_identifier:
            print("ERROR: uniqueIdentifier is missing")
            return {
                'statusCode': 400,
                'body': json.dumps({
                    'error': 'uniqueIdentifier is required in the request body'
                })
            }
        
        print(f"uniqueIdentifier validation passed: {unique_identifier}")
        
        if not data:
            print("ERROR: data is missing")
            return {
                'statusCode': 400,
                'body': json.dumps({
                    'error': 'data is required in the request body'
                })
            }
        
        print(f"data validation passed: {type(data)}")
        
        holdings = data.get('holdings', [])
        print(f"Extracted holdings: {len(holdings)} items")
        
        if not holdings:
            print("ERROR: holdings array is empty or missing")
            return {
                'statusCode': 400,
                'body': json.dumps({
                    'error': 'holdings array is required in the data'
                })
            }
        
        print("Input validation completed successfully")
        
        print("Processing holdings for sector analysis")
        user_sectors = process_holdings_to_sectors(holdings)
        
        print("Calculating similarity with SP500 sectors")
        similarity = calculate_similarity(sp500_sectors, user_sectors)
        
        print("Getting bias analysis from Bedrock")
        bias_analysis = get_bias_analysis(sp500_sectors, user_sectors, similarity)
        
        response_data = {
            'unique_identifier': unique_identifier,
            'timestamp': datetime.utcnow().isoformat(),
            'sp500_sectors': sp500_sectors,
            'user_sectors': user_sectors,
            'similarity_percentage': similarity,
            'bias_analysis': bias_analysis
        }
        
        bucket_name = 'hidden-for-github'
        s3_key = f'results/{unique_identifier}/sector_results.json'
        print(f"Saving results to S3: s3://{bucket_name}/{s3_key}")
        
        success = save_to_s3(bucket_name, s3_key, response_data)
        
        if success:
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': 'Analysis completed successfully',
                    'unique_identifier': unique_identifier,
                    's3_location': f's3://{bucket_name}/{s3_key}'
                })
            }
        else:
            return {
                'statusCode': 500,
                'body': json.dumps({
                    'error': 'Failed to save results to S3'
                })
            }
        
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': f'Internal server error: {str(e)}'
            })
        }