import os
import requests
from google.cloud import storage

# SETTINGS
BUCKET_NAME = "yellow_tripdata_md"
MONTHS = [f"{i:02d}" for i in range(1, 7)]  # 01, 02, 03, 04, 05, 06

def download_and_upload():
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NAME)

    for month in MONTHS:
        file_name = f"yellow_tripdata_2024-{month}.parquet"
        url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{file_name}"
        
        print(f"--- Processing {file_name} ---")
        
        # Download the file
        print(f"Downloading: {url}")
        
        if response.status_code == 200:
            # Upload to GCS (uploading directly from memory, no local disk usage)
            blob = bucket.blob(file_name)
            print(f"Uploading to GCS: {BUCKET_NAME}/{file_name}")
            blob.upload_from_string(
                response.content,
                content_type='application/octet-stream'
            )
            print(f"Completed: {file_name}\n")
        else:
            print(
                f"Error: {file_name} could not be downloaded! "
                f"(Status: {response.status_code})"
            )

if __name__ == "__main__":
    download_and_upload()
    print("All files have been uploaded successfully!")
