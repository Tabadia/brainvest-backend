import json
import os
import yfinance as yf
import logging
from datetime import datetime, timedelta
import time
import urllib.parse
import random
import requests
import hmac
import hashlib
import base64
from yfinance import set_tz_cache_location

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Bunch of vercel timezone stuff
set_tz_cache_location('/tmp')
os.environ['TZ'] = 'UTC'

try:
    import zoneinfo
    zoneinfo.ZoneInfo('UTC')
    logger.info("Timezone data available")
except Exception as e:
    logger.warning(f"Timezone data not available: {str(e)}")

class CustomTicker(yf.Ticker):
    def __init__(self, ticker, session=None):
        super().__init__(ticker, session)
        self._tz = 'UTC'
    
    def _fetch_ticker_tz(self):
        return 'UTC'
    
    def _get_tz(self):
        return 'UTC'

# s3 calls
def create_aws_signature(method, uri, query_string, headers, payload, region='us-east-1', service='s3'):
    access_key = os.environ['AWS_ACCESS_KEY_ID']
    secret_key = os.environ['AWS_SECRET_ACCESS_KEY']
    
    now = datetime.utcnow()
    timestamp = now.strftime('%Y%m%dT%H%M%SZ')
    date_stamp = now.strftime('%Y%m%d')
    
    canonical_headers = '\n'.join([f"{k.lower()}:{v}" for k, v in sorted(headers.items())])
    signed_headers = ';'.join([k.lower() for k in sorted(headers.keys())])
    
    payload_hash = hashlib.sha256(payload.encode('utf-8')).hexdigest()
    
    canonical_request = f"{method}\n{uri}\n{query_string}\n{canonical_headers}\n\n{signed_headers}\n{payload_hash}"
    
    credential_scope = f"{date_stamp}/{region}/{service}/aws4_request"
    string_to_sign = f"AWS4-HMAC-SHA256\n{timestamp}\n{credential_scope}\n{hashlib.sha256(canonical_request.encode('utf-8')).hexdigest()}"
    
    def sign(key, msg):
        return hmac.new(key, msg.encode('utf-8'), hashlib.sha256).digest()
    
    k_date = sign(f"AWS4{secret_key}".encode('utf-8'), date_stamp)
    k_region = sign(k_date, region)
    k_service = sign(k_region, service)
    k_signing = sign(k_service, 'aws4_request')
    signature = sign(k_signing, string_to_sign)
    
    authorization = f"AWS4-HMAC-SHA256 Credential={access_key}/{credential_scope}, SignedHeaders={signed_headers}, Signature={signature.hex()}"
    
    return authorization, timestamp

def get_portfolio_from_s3_api(bucket, key):
    try:
        uri = f"/{key}"
        query_string = ""
        
        headers = {
            'Host': f"{bucket}.s3.amazonaws.com",
            'x-amz-content-sha256': 'UNSIGNED-PAYLOAD'
        }
        
        authorization, timestamp = create_aws_signature('GET', uri, query_string, headers, '')
        headers['Authorization'] = authorization
        headers['x-amz-date'] = timestamp
        
        s3_url = f"https://{bucket}.s3.amazonaws.com/{key}"
        response = requests.get(s3_url, headers=headers)
        
        if response.status_code == 200:
            portfolio_data = response.json()
            logger.info(f"Successfully retrieved portfolio from S3: s3://{bucket}/{key}")
            return portfolio_data
        else:
            logger.error(f"Failed to retrieve from S3: {response.status_code} - {response.text}")
            return None
            
    except Exception as e:
        logger.error(f"Error retrieving portfolio from S3: {str(e)}")
        return None

def upload_to_s3_api(enriched_portfolio, bucket_name=None, key=None):
    if bucket_name is None:
        bucket_name = os.environ.get('S3_BUCKET_NAME', 'hidden-for-github')
    
    try:
        if key is None:
            uniqueIdentifier = f"direct_input_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            fileName = "portfolio"
        else:
            parts = key.split("/")
            if len(parts) >= 3:
                folder_name = parts[1] # get the unique identifier!!
                if folder_name.startswith("upload_"):
                    uniqueIdentifier = folder_name.replace("upload_", "")
                else:
                    uniqueIdentifier = folder_name
                fileName = parts[-1].rsplit(".", 1)[0]
            else:
                raise ValueError("Invalid S3 key format")
        
        filename = f"processed/{uniqueIdentifier}/{fileName}.json"
        portfolio_json = json.dumps(enriched_portfolio, default=str, indent=2)
        
        uri = f"/{filename}"
        query_string = ""
        
        headers = {
            'Host': f"{bucket_name}.s3.amazonaws.com",
            'Content-Type': 'application/json',
            'Content-Length': str(len(portfolio_json)),
            'x-amz-content-sha256': hashlib.sha256(portfolio_json.encode('utf-8')).hexdigest()
        }
        
        authorization, timestamp = create_aws_signature('PUT', uri, query_string, headers, portfolio_json)
        headers['Authorization'] = authorization
        headers['x-amz-date'] = timestamp
        
        s3_url = f"https://{bucket_name}.s3.amazonaws.com/{filename}"
        response = requests.put(s3_url, data=portfolio_json, headers=headers)
        
        if response.status_code == 200:
            logger.info(f"Successfully uploaded to s3://{bucket_name}/{filename}")
            return {
                'success': True,
                'bucket': bucket_name,
                'key': filename,
                'size': len(portfolio_json)
            }
        else:
            logger.error(f"S3 upload failed: {response.status_code} - {response.text}")
            return {'success': False, 'error': f"HTTP {response.status_code}"}
        
    except Exception as e:
        logger.error(f"S3 upload error: {str(e)}")
        return {'success': False, 'error': str(e)}


def get_asset_type(info, ticker_symbol=None):
    quote_type = info.get('quoteType', '').lower()
    category = info.get('category', '').lower()
    long_name = info.get('longName', '').lower()
    
    # cash detection
    if ticker_symbol and ticker_symbol.lower() == 'cash':
        return 'CASH'
    if any(word in long_name for word in ['cash', 'money market', 'treasury bill']):
        return 'CASH'
    
    # bond detection
    if 'bond' in quote_type or 'bond' in category or 'bond' in long_name or 'fixed income' in category:
        return 'BOND'
    
    # etf detection
    if quote_type == 'etf' or 'etf' in long_name:
        return 'ETF'
    
    return 'STOCK'

def get_ticker_info(ticker_symbol, max_retries=2, base_delay=5):
    for attempt in range(max_retries):
        try:
            delay = base_delay * (attempt + 1) + random.uniform(2, 4)
            logger.info(f"Fetching info for {ticker_symbol}, waiting {delay:.2f}s")
            time.sleep(delay)
            
            ticker = CustomTicker(ticker_symbol)
            info = ticker.info
            
            if info and len(info) > 1:
                logger.info(f"Successfully retrieved info for {ticker_symbol}")
                return ticker, info
            else:
                logger.warning(f"Empty response for {ticker_symbol}")
                
        except Exception as e:
            error_msg = str(e)
            logger.error(f"Error for {ticker_symbol}: {error_msg}")
            
            if any(phrase in error_msg.lower() for phrase in ['delisted', 'not found', 'invalid', 'timezone']):
                logger.error(f"Non-retryable error for {ticker_symbol}: {error_msg}")
                return None, None
                
            # rate limiting :(
            if "429" in error_msg or "Too Many Requests" in error_msg:
                logger.error(f"Rate limited for {ticker_symbol}: {error_msg}")
                if attempt < max_retries - 1:
                    time.sleep(15) 
                    continue
            else:
                if attempt < max_retries - 1:
                    time.sleep(10)
                    continue
    
    logger.error(f"Failed to get info for {ticker_symbol} after {max_retries} attempts")
    return None, None

def get_historical_data_single(symbol, max_retries=2):
    for attempt in range(max_retries):
        try:
            delay = random.uniform(3, 5)
            logger.info(f"Getting historical data for {symbol}, waiting {delay:.2f}s")
            time.sleep(delay)
            
            end_date = datetime.now()
            if end_date.month <= 2:
                start_date = end_date.replace(year=end_date.year-1, month=end_date.month+10)
            else:
                start_date = end_date.replace(month=end_date.month-2)
            
            days_diff = (end_date - start_date).days
            if days_diff < 60:
                start_date = end_date - timedelta(days=60)
            
            ticker = CustomTicker(symbol)
            
            try:
                hist = ticker.history(period="2mo", interval='1d')
                if hist is not None and not hist.empty:
                    logger.info(f"Successfully retrieved historical data for {symbol} using period method")
                    return hist
            except Exception as period_error:
                logger.warning(f"Period method failed for {symbol}: {str(period_error)}")
                
                try:
                    hist = ticker.history(start=start_date, end=end_date, interval='1d')
                    if hist is not None and not hist.empty:
                        logger.info(f"Successfully retrieved historical data for {symbol} using date range method")
                        return hist
                except Exception as date_error:
                    logger.warning(f"Date range method failed for {symbol}: {str(date_error)}")
                    
                    try:
                        hist = ticker.history(period="2mo", interval='1wk')
                        if hist is not None and not hist.empty:
                            logger.info(f"Successfully retrieved historical data for {symbol} using weekly interval")
                            return hist
                    except Exception as weekly_error:
                        logger.warning(f"Weekly interval method failed for {symbol}: {str(weekly_error)}")
                        raise period_error
            
            logger.warning(f"Empty historical data for {symbol}")
            return None
                
        except Exception as e:
            error_msg = str(e)
            logger.error(f"Error getting historical data for {symbol}: {error_msg}")
            
            if any(phrase in error_msg.lower() for phrase in ['delisted', 'not found', 'invalid', 'timezone']):
                logger.error(f"Non-retryable error for {symbol}: {error_msg}")
                return None
                
            if "429" in error_msg or "Too Many Requests" in error_msg:
                logger.error(f"Rate limited for {symbol}: {error_msg}")
                if attempt < max_retries - 1:
                    time.sleep(15)
                    continue
            else:
                if attempt < max_retries - 1:
                    time.sleep(8)
                    continue
    
    logger.error(f"Failed to get historical data for {symbol} after {max_retries} attempts")
    return None

def enrich_holdings_batch(holdings):
    logger.info(f"Starting batch enrichment for {len(holdings)} holdings")
    
    enriched_holdings = []
    
    for i, holding in enumerate(holdings):
        symbol = holding.get("symbol")
        if not symbol:
            logger.warning("Skipping holding with no symbol")
            continue
            
        logger.info(f"Enriching {symbol} ({i+1}/{len(holdings)})")
        
        enriched_data = holding.copy()
        enriched_data["analysis"] = {
            "timestamp": datetime.now().isoformat()
        }
        
        try:
            ticker, info = get_ticker_info(symbol)
            
            if ticker is None or info is None:
                enriched_data["analysis"]["error"] = "Failed to retrieve data"
                enriched_holdings.append(enriched_data)
                continue
            
            asset_type = get_asset_type(info, symbol)
            enriched_data["analysis"]["asset_type"] = asset_type
            
            # location
            if asset_type == 'STOCK':
                sector = info.get('sector')
                industry = info.get('industry')
                enriched_data["analysis"]["sector"] = sector if sector else "Unknown"
                enriched_data["analysis"]["industry"] = industry if industry else "Unknown"
            elif asset_type == 'ETF':
                enriched_data["analysis"]["category"] = info.get('category', 'Unknown')
            
            if asset_type == 'STOCK':
                country = info.get('country')
                city = info.get('city')
                state = info.get('state')
                
                enriched_data["analysis"]["hq_location"] = {
                    "country": country if country else "Unknown",
                    "city": city if city else "Unknown",
                    "state": state if state else None
                }
            
            # prime momentum
            if asset_type == 'STOCK':
                try:
                    hist = get_historical_data_single(symbol)
                    if hist is not None and not hist.empty and len(hist) >= 20:
                        month_ago_price = hist['Close'].iloc[-21] if len(hist) >= 21 else hist['Close'].iloc[0]
                        current_price = hist['Close'].iloc[-1]
                        stock_return = ((current_price - month_ago_price) / month_ago_price) * 100
                        
                        enriched_data["analysis"]["price_momentum"] = {
                            "stock_return_1m": round(stock_return, 2)
                        }
                except Exception as e:
                    logger.warning(f"Failed to calculate momentum for {symbol}: {str(e)}")
            
            logger.info(f"Successfully enriched {symbol}")
            enriched_holdings.append(enriched_data)
            
            if i < len(holdings) - 1:
                delay = random.uniform(5, 8)
                logger.info(f"Waiting {delay:.2f}s before next ticker")
                time.sleep(delay)
            
        except Exception as e:
            logger.error(f"Error enriching {symbol}: {str(e)}")
            enriched_data["analysis"]["error"] = str(e)
            enriched_holdings.append(enriched_data)
    
    logger.info(f"Batch enrichment complete: {len(enriched_holdings)} holdings processed")
    return enriched_holdings

def handler(request):
    try:
        logger.info("Starting portfolio enrichment")
        
        if request.method == 'OPTIONS':
            return {
                'statusCode': 200,
                'headers': {
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Methods': 'POST, GET, OPTIONS',
                    'Access-Control-Allow-Headers': 'Content-Type, Authorization',
                },
                'body': ''
            }
        
        if request.method == 'POST':
            try:
                portfolio_data = request.get_json()
            except:
                return {
                    'statusCode': 400,
                    'headers': {
                        'Access-Control-Allow-Origin': '*',
                        'Content-Type': 'application/json'
                    },
                    'body': json.dumps({'error': 'Invalid JSON in request body'})
                }
        else:
            bucket = request.args.get('bucket')
            key = request.args.get('key')
            
            if bucket and key:
                portfolio_data = get_portfolio_from_s3_api(bucket, key)
                if portfolio_data is None:
                    return {
                        'statusCode': 400,
                        'headers': {
                            'Access-Control-Allow-Origin': '*',
                            'Content-Type': 'application/json'
                        },
                        'body': json.dumps({'error': 'Failed to retrieve portfolio from S3'})
                    }
            else:
                return {
                    'statusCode': 400,
                    'headers': {
                        'Access-Control-Allow-Origin': '*',
                        'Content-Type': 'application/json'
                    },
                    'body': json.dumps({'error': 'Missing portfolio data or S3 parameters'})
                }
        
        if 'holdings' not in portfolio_data:
            return {
                'statusCode': 400,
                'headers': {
                    'Access-Control-Allow-Origin': '*',
                    'Content-Type': 'application/json'
                },
                'body': json.dumps({'error': 'Missing holdings array'})
            }
        
        enriched_holdings = enrich_holdings_batch(portfolio_data["holdings"])
        
        enriched_portfolio = {
            "metadata": portfolio_data.get("metadata", {}),
            "account_summary": portfolio_data.get("account_summary", {}),
            "holdings": enriched_holdings,
            "enrichment_metadata": {
                "processed_at": datetime.now().isoformat(),
                "total_holdings": len(portfolio_data["holdings"])
            }
        }
        
        logger.info(f"Enrichment complete: {len(enriched_portfolio['holdings'])} holdings processed")
        
        s3_result = None
        if os.environ.get('UPLOAD_TO_S3', 'true').lower() == 'true':
            s3_result = upload_to_s3_api(enriched_portfolio)
        
        return {
            'statusCode': 200,
            'headers': {
                'Access-Control-Allow-Origin': '*',
                'Content-Type': 'application/json'
            },
            'body': json.dumps({
                'status': 'complete',
                's3_upload': s3_result,
                'holdings_processed': len(enriched_portfolio['holdings']),
                'enriched_portfolio': enriched_portfolio
            }, default=str)
        }
        
    except Exception as e:
        logger.error(f"Function error: {str(e)}")
        return {
            'statusCode': 500,
            'headers': {
                'Access-Control-Allow-Origin': '*',
                'Content-Type': 'application/json'
            },
            'body': json.dumps({'error': str(e)})
        }