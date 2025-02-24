import requests
import os

# GCS Public File URL
GCS_FILE_URL = "https://storage.googleapis.com/data-analysis-project-bucket-bharat/uber_data.csv"

# Local storage path
LOCAL_FILE_PATH = "data/raw_data.csv"

def download_from_gcs():
    """Downloads a CSV file from a public GCS URL and saves it locally."""
    response = requests.get(GCS_FILE_URL)
    
    if response.status_code == 200:
        os.makedirs(os.path.dirname(LOCAL_FILE_PATH), exist_ok=True)
        with open(LOCAL_FILE_PATH, "w", encoding="utf-8") as f:
            f.write(response.text)
        print(f"Downloaded file saved at {LOCAL_FILE_PATH}")
    else:
        print(f"Failed to download file, Status Code: {response.status_code}")

if __name__ == "__main__":
    download_from_gcs()