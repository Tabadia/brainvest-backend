import json
import boto3
from typing import Dict, Any, List
from datetime import datetime

def process_holdings_to_locations(holdings: List[Dict[str, Any]]) -> Dict[str, float]:
    print(f"Processing {len(holdings)} holdings for location analysis")
    location_allocations = {}
    total_percentage = 0.0
    processed_count = 0
    
    for i, holding in enumerate(holdings):
        try:
            country = holding.get('country', '').strip() if holding.get('country') else ''
            city = holding.get('city', '').strip() if holding.get('city') else ''
            state = holding.get('state', '').strip() if holding.get('state') else ''
            percentage = holding.get('portfolio_percentage', 0.0)
            
            if percentage <= 0:
                print(f"Skipping holding {i}: percentage {percentage} <= 0")
                continue
            
            if not country and not city and not state:
                print(f"Skipping holding {i}: no location data")
                continue
                
            total_percentage += percentage
            processed_count += 1
            
            if not country:
                country = 'Unknown'
            
            if state:
                location = f"{state}, {country}"
            elif city:
                location = f"{city}, {country}"
            else:
                location = country
            
            if location in location_allocations:
                location_allocations[location] += percentage
            else:
                location_allocations[location] = percentage
                
        except Exception as e:
            print(f"Error processing holding {i}: {str(e)}")
            continue
    
    print(f"Successfully processed {processed_count} out of {len(holdings)} holdings")
    
    if total_percentage > 0:
        location_allocations = {
            location: (percentage / total_percentage) * 100 
            for location, percentage in location_allocations.items()
        }
        print(f"Normalized location allocations. Total percentage: {total_percentage:.2f}%")
    
    print(f"Location analysis complete. Found {len(location_allocations)} unique locations")
    return location_allocations

def create_weighted_location_list(user_locations: Dict[str, float]) -> List[Dict[str, Any]]:
    """
    Create a weighted list of locations sorted by percentage allocation.
    """
    print(f"Creating weighted location list from {len(user_locations)} locations")
    weighted_locations = []
    
    for location, percentage in user_locations.items():
        weighted_locations.append({
            "location": location,
            "percentage": round(percentage, 2)
        })
    
    # Sort by percentage in descending order
    weighted_locations.sort(key=lambda x: x['percentage'], reverse=True)
    
    return weighted_locations

def save_to_s3(bucket_name: str, key: str, data: Dict[str, Any]) -> bool:
    """
    Save data to S3 bucket as JSON file.
    """
    try:
        print(f"Attempting to save to S3 bucket: {bucket_name}, key: {key}")
        s3 = boto3.client('s3')
        
        # Check if bucket exists
        try:
            s3.head_bucket(Bucket=bucket_name)
            print(f"S3 bucket '{bucket_name}' exists and is accessible")
        except Exception as bucket_error:
            print(f"Error accessing S3 bucket '{bucket_name}': {str(bucket_error)}")
            return False
        
        # Attempt to put object
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
    """
    AWS Lambda handler function that processes user location allocations.
    Returns weighted location list in JSON format.
    """
    print("Starting location analysis lambda handler")
    try:
        # Extract unique identifier and data from the event
        unique_identifier = event.get('uniqueIdentifier')
        data = event.get('data', {})
        print(f"Processing request for unique identifier: {unique_identifier}")
        
        # Validate input
        print("Starting input validation")
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
        
        # Extract holdings from data
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
        
        # Validate portfolio percentages are reasonable
        print("Validating portfolio percentages")
        total_percentage = sum(holding.get('portfolio_percentage', 0.0) for holding in holdings)
        print(f"Total portfolio percentage: {total_percentage:.2f}%")
        
        # Check for reasonable percentage range (0-100% per holding, total can be less than 100% due to ETFs/bonds)
        for i, holding in enumerate(holdings):
            percentage = holding.get('portfolio_percentage', 0.0)
            
            # Validate data type
            if not isinstance(percentage, (int, float)):
                print(f"ERROR: Invalid data type for portfolio_percentage in holding {i}: {type(percentage)}")
                return {
                    'statusCode': 400,
                    'body': json.dumps({
                        'error': f'Portfolio percentage must be a number. Found {type(percentage).__name__} in holding {i}'
                    })
                }
            
            if percentage < 0 or percentage > 100:
                print(f"ERROR: Invalid portfolio percentage: {percentage:.2f}% for holding {i}")
                return {
                    'statusCode': 400,
                    'body': json.dumps({
                        'error': f'Portfolio percentages must be between 0% and 100%. Found: {percentage:.2f}% in holding {i}'
                    })
                }
        
        if total_percentage > 100:
            print(f"ERROR: Total portfolio percentage exceeds 100%. Current sum: {total_percentage:.2f}%")
            return {
                'statusCode': 400,
                'body': json.dumps({
                    'error': f'Total portfolio percentage cannot exceed 100%. Current sum: {total_percentage:.2f}%'
                })
            }
        
        print("Portfolio percentage validation passed")
        
        # Validate location fields in holdings
        print("Validating location fields")
        for i, holding in enumerate(holdings):
            country = holding.get('country', '')
            city = holding.get('city', '')
            state = holding.get('state', '')
            
            # Check if at least one location field is provided
            if not country and not city and not state:
                print(f"WARNING: Holding {i} has no location data (country, city, or state)")
            elif not country:
                print(f"WARNING: Holding {i} missing country field")
        
        print("Location field validation completed")
        
        # Process holdings to get location allocations
        print("Processing holdings for location analysis")
        user_locations = process_holdings_to_locations(holdings)
        
        # Check if any valid locations were found
        if not user_locations:
            print("ERROR: No valid locations found after processing holdings")
            return {
                'statusCode': 400,
                'body': json.dumps({
                    'error': 'No valid locations found. Please ensure holdings have valid location data (country, city, or state) and positive portfolio percentages.'
                })
            }
        
        # Create weighted location list
        print("Creating weighted location list")
        weighted_locations = create_weighted_location_list(user_locations)
        
        response_data = {
            'unique_identifier': unique_identifier,
            'timestamp': datetime.utcnow().isoformat(),
            'weighted_locations': weighted_locations
        }
        
        bucket_name = 'hidden-for-github'
        s3_key = f'results/{unique_identifier}/location_results.json'
        print(f"Saving results to S3: s3://{bucket_name}/{s3_key}")
        
        success = save_to_s3(bucket_name, s3_key, response_data)
        
        if success:
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': 'Location analysis completed successfully',
                    'unique_identifier': unique_identifier,
                    'weighted_locations': weighted_locations,
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
