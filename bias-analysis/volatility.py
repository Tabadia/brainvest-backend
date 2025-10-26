import json
import boto3
from datetime import datetime

s3_client = boto3.client('s3')
RESULTS_BUCKET = "hidden-for-github"


def get_risk_analysis(weighted_beta: float, weighted_sharpe: float) -> str:
    print(f"Getting risk analysis from Bedrock for beta: {weighted_beta:.4f}, sharpe: {weighted_sharpe:.4f}")
    bedrock = boto3.client('bedrock-runtime', region_name='us-east-1')
    
    prompt = f"""
    Analyze the following portfolio risk metrics and provide a brief, easy-to-understand insight about this portfolio's risk-return profile:
    
    Portfolio Weighted Beta: {weighted_beta:.4f}
    Portfolio Weighted Sharpe Ratio: {weighted_sharpe:.4f}
    
    IMPORTANT: Risk interpretation guidelines:
    - Beta > 1.5: High market sensitivity (aggressive/risky approach)
    - Beta 0.8-1.5: Moderate market sensitivity (balanced approach)
    - Beta < 0.8: Low market sensitivity (defensive/conservative approach)
    
    - Sharpe > 1.5: Excellent risk-adjusted returns (high reward for risk taken)
    - Sharpe 0.5-1.5: Good risk-adjusted returns (reasonable reward for risk)
    - Sharpe < 0.5: Poor risk-adjusted returns (low reward for risk taken)
    
    Please provide a brief one-sentence analysis explaining what this portfolio's beta and Sharpe values mean in simple terms, focusing on whether the portfolio is taking appropriate risks and getting rewarded for them.
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
        print("Successfully received risk analysis from Bedrock")
        return analysis
    
    except Exception as e:
        print(f"Error getting analysis from Bedrock: {str(e)}")
        raise Exception(f"Failed to get analysis from Bedrock: {str(e)}")


def lambda_handler(event, context):
    try:
        print("Starting weighted beta calculation...")
        
        uniqueIdentifier = event.get("uniqueIdentifier")
        data = event.get("data", {})
        holdings = data.get("holdings", [])
        
        weighted_beta = 0
        weighted_sharpe = 0
        asset_type_count = {}
        
        for holding in holdings:
            symbol = holding.get('symbol')
            portfolio_pct = holding.get('portfolio_percentage', 0)
            beta = holding.get('beta') or 0
            sharpe = holding.get('sharpe') or 0
            asset_type = holding.get('asset_type', 'Unknown')

            asset_type_count[asset_type] = asset_type_count.get(asset_type, 0) + 1

            weight = portfolio_pct / 100.0
            contribution_beta = weight * beta
            contribution_sharpe = weight * sharpe
            
            weighted_beta += contribution_beta
            weighted_sharpe += contribution_sharpe
    
        print(f"Portfolio weighted beta: {weighted_beta:.4f}")
        print(f"Portfolio weighted sharpe: {weighted_sharpe:.4f}")
        print(f"Asset types: {asset_type_count}")

        print("Getting risk analysis from Bedrock")
        risk_analysis = get_risk_analysis(weighted_beta, weighted_sharpe)

        result = {
            "unique_identifier": uniqueIdentifier,
            "timestamp": datetime.utcnow().isoformat(),
            "weighted_beta": weighted_beta,
            "weighted_sharpe": weighted_sharpe,
            "asset_types": asset_type_count,
            "risk_analysis": risk_analysis
        }

        s3_key = f"results/{uniqueIdentifier}/volatility_results.json"

        s3_client.put_object(
            Bucket=RESULTS_BUCKET,
            Key=s3_key,
            Body=json.dumps(result),
            ContentType='application/json'
        )
        
        
    except Exception as e:
        print(f"Error: {str(e)}")
        return {'statusCode': 500, 'body': json.dumps({'error': str(e)})}