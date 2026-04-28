import json
import requests
import boto3
from datetime import datetime

# CONFIGURATION - CHANGE THIS TO YOUR BUCKET NAME
BUCKET_NAME = "market-data-analysis-project-muskanprabin" 

def fetch_and_upload():
    # 1. Fetch data from API
    url = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin,ethereum,solana&vs_currencies=usd&include_24hr_vol=true"
    response = requests.get(url)
    data = response.json()
    
    # 2. Add a timestamp (crucial for time-series analysis!)
    data['timestamp'] = datetime.now().isoformat()
    
    # 3. Connect to S3 and upload
    s3 = boto3.client('s3')
    file_name = f"raw-data/crypto_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    
    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=file_name,
        Body=json.dumps(data)
    )
    
    print(f"Successfully uploaded {file_name} to {BUCKET_NAME}")

if __name__ == "__main__":
    fetch_and_upload()
