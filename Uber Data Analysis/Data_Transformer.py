#Keys are in the other folder
import pandas as pd
import requests
import io
import os
from google.cloud import storage

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/Users/bharat_puri/Documents/Data-Analysis/Uber Data Analysis/data-analysis-project-bharat-e935acfd60da.json"
# Configuration
GCS_FILE_URL = "https://storage.googleapis.com/data-analysis-project-bucket-bharat/uber_data.csv"
BUCKET_NAME = "data-analysis-project-bucket-bharat"
DEST_BLOB_NAME = "processed/cleaned_data.csv"
LOCAL_CLEANED_FILE = "data/cleaned_data.csv"

def download_from_gcs():
    """Downloads raw CSV from GCS public URL into memory."""
    response = requests.get(GCS_FILE_URL)
    if response.status_code == 200:
        return io.StringIO(response.text)
    else:
        raise Exception(f"Failed to download file, Status Code: {response.status_code}")

def transform_data(csv_data):
    """Applies pandas transformations to clean and structure the data."""
    df = pd.read_csv(csv_data)
    
    # Convert to datetime
    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])
        
        # Remove duplicates
    df = df.drop_duplicates().reset_index(drop=True)
    df['trip_id'] = df.index
        
        # Create datetime dimensions
    datetime_dim = df[['tpep_pickup_datetime','tpep_dropoff_datetime']].reset_index(drop=True)
    datetime_dim['tpep_pickup_datetime'] = datetime_dim['tpep_pickup_datetime']
    datetime_dim['pick_hour'] = datetime_dim['tpep_pickup_datetime'].dt.hour
    datetime_dim['pick_day'] = datetime_dim['tpep_pickup_datetime'].dt.day
    datetime_dim['pick_month'] = datetime_dim['tpep_pickup_datetime'].dt.month
    datetime_dim['pick_year'] = datetime_dim['tpep_pickup_datetime'].dt.year
    datetime_dim['pick_weekday'] = datetime_dim['tpep_pickup_datetime'].dt.weekday

    datetime_dim['tpep_dropoff_datetime'] = datetime_dim['tpep_dropoff_datetime']
    datetime_dim['drop_hour'] = datetime_dim['tpep_dropoff_datetime'].dt.hour
    datetime_dim['drop_day'] = datetime_dim['tpep_dropoff_datetime'].dt.day
    datetime_dim['drop_month'] = datetime_dim['tpep_dropoff_datetime'].dt.month
    datetime_dim['drop_year'] = datetime_dim['tpep_dropoff_datetime'].dt.year
    datetime_dim['drop_weekday'] = datetime_dim['tpep_dropoff_datetime'].dt.weekday


    datetime_dim['datetime_id'] = datetime_dim.index

    # datetime_dim = datetime_dim.rename(columns={'tpep_pickup_datetime': 'datetime_id'}).reset_index(drop=True)
    datetime_dim = datetime_dim[['datetime_id', 'tpep_pickup_datetime', 'pick_hour', 'pick_day', 'pick_month', 'pick_year', 'pick_weekday',
                                'tpep_dropoff_datetime', 'drop_hour', 'drop_day', 'drop_month', 'drop_year', 'drop_weekday']]
        
    passenger_count_dim = df[['passenger_count']].reset_index(drop=True)
    passenger_count_dim['passenger_count_id'] = passenger_count_dim.index
    passenger_count_dim = passenger_count_dim[['passenger_count_id','passenger_count']]

    trip_distance_dim = df[['trip_distance']].reset_index(drop=True)
    trip_distance_dim['trip_distance_id'] = trip_distance_dim.index
    trip_distance_dim = trip_distance_dim[['trip_distance_id','trip_distance']]

    rate_code_type = {
        1:"Standard rate",
        2:"JFK",
        3:"Newark",
        4:"Nassau or Westchester",
        5:"Negotiated fare",
        6:"Group ride"
    }

    rate_code_dim = df[['RatecodeID']].reset_index(drop=True)
    rate_code_dim['rate_code_id'] = rate_code_dim.index
    rate_code_dim['rate_code_name'] = rate_code_dim['RatecodeID'].map(rate_code_type)
    rate_code_dim = rate_code_dim[['rate_code_id','RatecodeID','rate_code_name']]

    pickup_location_dim = df[['pickup_longitude', 'pickup_latitude']].reset_index(drop=True)
    pickup_location_dim['pickup_location_id'] = pickup_location_dim.index
    pickup_location_dim = pickup_location_dim[['pickup_location_id','pickup_latitude','pickup_longitude']] 


    dropoff_location_dim = df[['dropoff_longitude', 'dropoff_latitude']].reset_index(drop=True)
    dropoff_location_dim['dropoff_location_id'] = dropoff_location_dim.index
    dropoff_location_dim = dropoff_location_dim[['dropoff_location_id','dropoff_latitude','dropoff_longitude']]

    payment_type_name = {
        1:"Credit card",
        2:"Cash",
        3:"No charge",
        4:"Dispute",
        5:"Unknown",
        6:"Voided trip"
    }
    payment_type_dim = df[['payment_type']].reset_index(drop=True)
    payment_type_dim['payment_type_id'] = payment_type_dim.index
    payment_type_dim['payment_type_name'] = payment_type_dim['payment_type'].map(payment_type_name)
    payment_type_dim = payment_type_dim[['payment_type_id','payment_type','payment_type_name']]

    # Save cleaned data locally
    tables = {
        "datetime_dim": df[['tpep_pickup_datetime','tpep_dropoff_datetime']].reset_index(drop=True),
        "passenger_count_dim": df[['passenger_count']].reset_index(drop=True),
        "trip_distance_dim": df[['trip_distance']].reset_index(drop=True),
        "rate_code_dim": df[['RatecodeID']].reset_index(drop=True),
        "pickup_location_dim": df[['pickup_longitude', 'pickup_latitude']].reset_index(drop=True),
        "dropoff_location_dim": df[['dropoff_longitude', 'dropoff_latitude']].reset_index(drop=True),
        "payment_type_dim": df[['payment_type']].reset_index(drop=True)
    }

    # Add IDs
    for table_name, df_table in tables.items():
        df_table[f"{table_name}_id"] = df_table.index

    # Create Fact Table
    fact_table = df.merge(tables["passenger_count_dim"], left_on='trip_id', right_on='passenger_count_dim_id') \
                   .merge(tables["trip_distance_dim"], left_on='trip_id', right_on='trip_distance_dim_id') \
                   .merge(tables["rate_code_dim"], left_on='trip_id', right_on='rate_code_dim_id') \
                   .merge(tables["pickup_location_dim"], left_on='trip_id', right_on='pickup_location_dim_id') \
                   .merge(tables["dropoff_location_dim"], left_on='trip_id', right_on='dropoff_location_dim_id') \
                   .merge(tables["datetime_dim"], left_on='trip_id', right_on='datetime_dim_id') \
                   .merge(tables["payment_type_dim"], left_on='trip_id', right_on='payment_type_dim_id') \
                   [['trip_id', 'datetime_dim_id', 'passenger_count_dim_id', 'trip_distance_dim_id', 'rate_code_dim_id',
                     'pickup_location_dim_id', 'dropoff_location_dim_id', 'payment_type_dim_id', 'fare_amount',
                     'extra', 'mta_tax', 'tip_amount', 'tolls_amount', 'improvement_surcharge', 'total_amount']]
    
    tables["fact_table"] = fact_table

    # Save tables locally
    os.makedirs("data", exist_ok=True)
    for table_name, df_table in tables.items():
        df_table.to_csv(f"data/{table_name}.csv", index=False)
        print(f"✅ {table_name} saved locally.")

    return tables

def upload_to_gcs():
    """Uploads cleaned CSV to Google Cloud Storage."""
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(DEST_BLOB_NAME)
    blob.upload_from_filename(LOCAL_CLEANED_FILE)
    print(f"Uploaded cleaned file to gs://{BUCKET_NAME}/{DEST_BLOB_NAME}")

if __name__ == "__main__":
    raw_csv_data = download_from_gcs()
    cleaned_file = transform_data(raw_csv_data)
    upload_to_gcs()