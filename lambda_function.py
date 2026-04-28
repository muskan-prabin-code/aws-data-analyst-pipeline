import json
import boto3
import csv
import io

s3 = boto3.client('s3')

def lambda_handler(event, context):
    # 1. Get the bucket and file name from the S3 event
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    
    # 2. Read the JSON file
    response = s3.get_object(Bucket=bucket, Key=key)
    raw_data = json.loads(response['Body'].read().decode('utf-8'))
    
    # 3. Transform: Flatten the JSON
    timestamp = raw_data.pop('timestamp')
    rows = []
    
    for coin_id, values in raw_data.items():
        rows.append({
            'coin': coin_id,
            'price_usd': values['usd'],
            'vol_24h': values['usd_24h_vol'],
            'timestamp': timestamp
        })
    
    # 4. Convert to CSV format
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=['coin', 'price_usd', 'vol_24h', 'timestamp'])
    writer.writeheader()
    writer.writerows(rows)
    
    # 5. Upload to the 'processed-data/' folder
    new_key = key.replace('raw-data/', 'processed-data/').replace('.json', '.csv')
    s3.put_object(
        Bucket=bucket,
        Key=new_key,
        Body=output.getvalue()
    )
    
    return {
        'statusCode': 200,
        'body': f"Successfully processed {key} to {new_key}"
    }
