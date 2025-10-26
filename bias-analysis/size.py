import json
import boto3
import math

s3_client = boto3.client('s3')
RESULTS_BUCKET = "hidden-for-github"

def cosine_similarity(portfolio, market):
    # categorize it
    caps = ['Mega-cap', 'Large-cap', 'Mid-cap', 'Small-cap', 'Micro-cap', 'Nano-cap']
    a = [portfolio.get(c, 0) for c in caps]
    b = [market.get(c, 0) for c in caps]

    dot = sum(x*y for x, y in zip(a, b))
    norm_a = math.sqrt(sum(x**2 for x in a))
    norm_b = math.sqrt(sum(y**2 for y in b))

    if norm_a == 0 or norm_b == 0:
        return 0.0
    return round((dot / (norm_a * norm_b)) * 100, 2)

def parse_market_cap(market_cap_str):
        if not market_cap_str:
            return 0
        market_cap_str = market_cap_str.upper().replace(',', '').strip()
        multiplier = 1
        if market_cap_str.endswith('T'):
            multiplier = 1_000_000_000_000
            market_cap_str = market_cap_str[:-1]
        elif market_cap_str.endswith('B'):
            multiplier = 1_000_000_000
            market_cap_str = market_cap_str[:-1]
        elif market_cap_str.endswith('M'):
            multiplier = 1_000_000
            market_cap_str = market_cap_str[:-1]
        try:
            return float(market_cap_str) * multiplier
        except:
            return 0


def lambda_handler(event, context):
    try:
        uniqueIdentifier = event.get("uniqueIdentifier")
        data = event.get("data", {})
        holdings = data.get("holdings", [])
        
        cap_counts = {
            'Mega-cap': 0,
            'Large-cap': 0,
            'Mid-cap': 0,
            'Small-cap': 0,
            'Micro-cap': 0,
            'Nano-cap': 0
        }

        total_holdings = len(holdings)

        
        for holding in holdings:
            market_cap = holding.get('market-cap')
            market_num = parse_market_cap(market_cap)
    
            if market_num > 200_000_000_000:
                cap_counts['Mega-cap'] += 1
            elif 10_000_000_000 <= market_num <= 200_000_000_000:
                cap_counts['Large-cap'] += 1
            elif 2_000_000_000 <= market_num < 10_000_000_000:
                cap_counts['Mid-cap'] += 1
            elif 300_000_000 <= market_num < 2_000_000_000:
                cap_counts['Small-cap'] += 1
            elif 50_000_000 <= market_num < 300_000_000:
                cap_counts['Micro-cap'] += 1
            else:
                cap_counts['Nano-cap'] += 1
        cap_percentages = {cap: round((count / total_holdings) * 100, 2) if total_holdings else 0 for cap, count in cap_counts.items()}
        print(cap_percentages)

        global_market = {
            'Mega-cap': 35,
            'Large-cap': 35,
            'Mid-cap': 20,
            'Small-cap': 5,
            'Micro-cap': 5,
            'Nano-cap': 0
        }

        similarity = cosine_similarity(cap_percentages, global_market)


        top_holdings = sorted(
            holdings,
            key=lambda x: x.get("total_gain_percent", 0),
            reverse=True
        )[:4]

        top_holdings_summary = [
            {
                "symbol": h.get("symbol"),
                "value": h.get("value"),
                "total_gain_percent": h.get("total_gain_percent")
            }
            for h in top_holdings
        ]


        result = {
            "market-cap": cap_percentages,
            "sp500": global_market,
            "similarity_percentage":similarity,
            "top_holdings": top_holdings_summary
        }


        s3_key = f"results/{uniqueIdentifier}/size_results.json"

        s3_client.put_object(
            Bucket=RESULTS_BUCKET,
            Key=s3_key,
            Body=json.dumps(result),
            ContentType='application/json'
        )
        
    except Exception as e:
        print(f"Error: {str(e)}")
        return {'statusCode': 500, 'body': json.dumps({'error': str(e)})}