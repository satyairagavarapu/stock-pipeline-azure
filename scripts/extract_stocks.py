import yfinance as yf
import json
import os
from datetime import datetime
from azure.storage.blob import BlobServiceClient

connection_string = os.environ["AZURE_STORAGE_CONNECTION_STRING"]
container_name = "bronze"

blob_service_client = BlobServiceClient.from_connection_string(connection_string)

stocks = ["TCS.NS", "RELIANCE.NS", "INFY.NS", "HDFCBANK.NS"]
today = datetime.today().strftime('%Y%m%d')

for symbol in stocks:
    ticker = yf.Ticker(symbol)
    hist = ticker.history(period="1mo")
    hist = hist.reset_index()
    hist['Date'] = hist['Date'].astype(str)
    data = hist.to_dict(orient='records')
    
    filename = f"{symbol.replace('.', '_')}_{today}.json"
    blob_path = f"raw_stocks/{filename}"
    
    blob_client = blob_service_client.get_blob_client(
        container=container_name,
        blob=blob_path
    )
    
    blob_json = json.dumps(data, indent=2)
    blob_client.upload_blob(blob_json, overwrite=True)
    print(f"Uploaded: {filename}")

print("Done!")