import json
import boto3
import yfinance as yf

s3_client = boto3.client('s3')
RESULTS_BUCKET = "hidden-for-github"

def lambda_handler(event, context):
    try:
        print("Starting weighted momentum calculation...")

        uniqueIdentifier = event.get("uniqueIdentifier")
        data = event.get("data", {})
        holdings = data.get("holdings", [])

        weighted_trailing_return = 0


        for holding in holdings:
            trailing_return_1m = holding.get('trailing_return_1m') or 0
            portfolio_percentage = holding.get('portfolio_percentage') or 0

            contribution = trailing_return_1m * (portfolio_percentage/100)
            weighted_trailing_return += contribution

        print(weighted_trailing_return)
        sp500_momentum = 3.5

        if weighted_trailing_return != 0:
            momentum_comparison = ((weighted_trailing_return - sp500_momentum) / sp500_momentum) * 100
        else:
            momentum_comparison = None

        print(f"Momentum comparison: {momentum_comparison}")

        result = {
            "price_momentum_comparison": momentum_comparison,
            "s&p500_momentum": sp500_momentum,
            "calculated_weighted_momentum": weighted_trailing_return
        }

        s3_key = f"results/{uniqueIdentifier}/momentum_results.json"

        s3_client.put_object(
            Bucket=RESULTS_BUCKET,
            Key=s3_key,
            Body=json.dumps(result),
            ContentType='application/json'
        )

    except Exception as e:
        print(f"Error: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }

    return {
        'statusCode': 200,
        'body': json.dumps('Momentum calculation complete!')
    }
