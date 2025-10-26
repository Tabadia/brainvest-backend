import json
import boto3
from urllib.parse import unquote_plus

s3_client = boto3.client('s3')
lambda_client = boto3.client('lambda')

def prepare_volatility_data(portfolio_data):
    holdings = portfolio_data.get('holdings', [])
    filtered_holdings = []
    
    for holding in holdings:
        analysis = holding.get('analysis', {})
        filtered_holdings.append({
            'symbol': holding.get('symbol'),
            'portfolio_percentage': holding.get('portfolio_percentage', 0),
            'beta': holding.get('beta'),
            'sharpe': analysis.get('sharpe_ratio'),
            'asset_type': analysis.get('asset_type')
        })
    print(f"Filtered holdings: {filtered_holdings}")
    return {'holdings': filtered_holdings}

def prepare_sector_data(portfolio_data):
    holdings = portfolio_data.get('holdings', [])
    filtered_holdings = []

    for holding in holdings:
        analysis = holding.get('analysis', {})
        asset_type = analysis.get('asset_type')

        if asset_type == 'ETF':
            continue
        else:
            filtered_holdings.append({
                'symbol': holding.get('symbol'),
                'portfolio_percentage': holding.get('portfolio_percentage', 0),
                'sector': analysis.get('sector'),
            })
    return {'holdings': filtered_holdings}

def prepare_size_data(portfolio_data):
    holdings = portfolio_data.get('holdings', [])
    filtered_holdings = []

    for holding in holdings:
        filtered_holdings.append({
            'symbol':holding.get('symbol'),
            'market-cap': holding.get('market_cap'),
            'total_gain_percent': holding.get('total_gain_percent'),
            'value': holding.get('value')
        })
    return {'holdings': filtered_holdings}

def prepare_location_data(portfolio_data):
    holdings = portfolio_data.get('holdings', [])
    filtered_holdings = []

    for holding in holdings:
        analysis = holding.get('analysis', {})
        asset_type = analysis.get('asset_type')

        if asset_type == 'ETF':
            continue
        else:
            company_info = analysis.get('hq_location', {})
            region = company_info.get('city') or company_info.get('state')

            filtered_holdings.append({
                'portfolio_percentage': holding.get('portfolio_percentage', 0),
                'country': company_info.get('country'),
                'region': region
            })
    return {'holdings': filtered_holdings}

def prepare_momentum_data(portfolio_data):
    holdings = portfolio_data.get('holdings', [])
    filtered_holdings = []
    
    for holding in holdings:
        analysis = holding.get('analysis', {})
        asset_type = analysis.get('asset_type')

        if asset_type == 'ETF':
            continue
        else:
            filtered_holdings.append({
                'portfolio_percentage': holding.get('portfolio_percentage', 0),
                'trailing_return_1m': analysis.get('trailing_return_1m')
            })
    print(f"Filtered holdings: {filtered_holdings}")
    return {'holdings': filtered_holdings}

def prepare_recency_data(portfolio_data):
    holdings = portfolio_data.get('holdings', [])
    filtered_holdings = []    
    for holding in holdings:
        analysis = holding.get('analysis', {})
        asset_type = analysis.get('asset_type')

        if asset_type == 'ETF':
            continue
        else:
            filtered_holdings.append({
                'symbol' :  holding.get("symbol")
            })
    print(f"Filtered holdings: {filtered_holdings}")
    return {'holdings': filtered_holdings}

def invoke_lambda(function_name, payload, uniqueIdentifier):
    try:
        event_payload = {
        "uniqueIdentifier": uniqueIdentifier,
        "data": payload
        }
        print(f"Invoking {function_name}...")
        lambda_client.invoke(
            FunctionName=function_name,
            InvocationType='Event',
            Payload= json.dumps(event_payload)
        )
        print(f"{function_name} invoked")
    except Exception as e:
        print(f"Error invoking {function_name}: {str(e)}")

def lambda_handler(event, context):
    try:
        print(f"Event received: {json.dumps(event)}")
        if 'Records' not in event:
            return {'statusCode': 400, 'body': json.dumps({'error': 'Invalid event format'})}
        
        record = event['Records'][0]
        source_bucket = record['s3']['bucket']['name']
        source_key = unquote_plus(record['s3']['object']['key'])

        parts = source_key.split("/")
        if len(parts) >= 3:
            uniqueIdentifier = parts[1] 
            fileName = parts[-1].rsplit(".", 1)[0] 
        else:
            raise ValueError(f"Invalid S3 key format: {source_key}")
        
        print(f"Processing: s3://{source_bucket}/{source_key}")
        
        # Download portfolio JSON from S3
        response = s3_client.get_object(Bucket=source_bucket, Key=source_key)
        portfolio_data = json.loads(response['Body'].read().decode('utf-8'))
        
        print(f"Loaded portfolio with {len(portfolio_data.get('holdings', []))} holdings")
        

        volatility_data = prepare_volatility_data(portfolio_data)
        invoke_lambda('portfolio-volatility-analysis', volatility_data, uniqueIdentifier)
        
        sector_data = prepare_sector_data(portfolio_data)
        invoke_lambda('portfolio-sector-analysis', sector_data, uniqueIdentifier)

        size_data = prepare_size_data(portfolio_data)
        invoke_lambda('portfolio-size-analysis', size_data, uniqueIdentifier)

        location_data = prepare_location_data(portfolio_data)
        invoke_lambda('portfolio-location-analysis', location_data, uniqueIdentifier)

        momentum_data = prepare_momentum_data(portfolio_data)
        invoke_lambda('portfolio-momentum-analysis', momentum_data, uniqueIdentifier)

        recency_data = prepare_recency_data(portfolio_data)

        event_payload= { 
            "uniqueIdentifier": uniqueIdentifier
        }
        lambda_client.invoke(
            FunctionName='result_compiler',
            InvocationType='Event',
            Payload= json.dumps(event_payload)
        )
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Portfolio analysis Lambdas invoked successfully',
                'source': f's3://{source_bucket}/{source_key}'
            })
        }
        
    except Exception as e:
        print(f"Error: {str(e)}")
        return {'statusCode': 500, 'body': json.dumps({'error': str(e)})}