#Keys are in the other folder
from google.cloud import bigquery, storage
import os

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/Users/bharat_puri/Documents/Data-Analysis/Uber Data Analysis/data-exporter-key.json"
# Configuration
BQ_PROJECT = "data-analysis-project-bharat"  # 🔄 Replace with your actual GCP project ID
BQ_DATASET = "uber_data_analysis_database"  # 🔄 Replace with your BigQuery dataset name
BUCKET_NAME = "data-analysis-project-bucket-bharat"  # 🔄 Replace with your actual GCS bucket name

# Define table names
BQ_TABLES = {
    "datetime_dim": "datetime_dim",
    "passenger_count_dim": "passenger_count_dim",
    "trip_distance_dim": "trip_distance_dim",
    "rate_code_dim": "rate_code_dim",
    "pickup_location_dim": "pickup_location_dim",
    "dropoff_location_dim": "dropoff_location_dim",
    "payment_type_dim": "payment_type_dim",
    "fact_table": "fact_table"
}

def load_data_to_bigquery():
    """Loads each table's CSV from GCS into BigQuery."""
    print("🔹 Loading tables into BigQuery...")
    client = bigquery.Client()

    for table_name, gcs_table_name in BQ_TABLES.items():
        table_id = f"{BQ_PROJECT}.{BQ_DATASET}.{gcs_table_name}"
        gcs_uri = f"gs://{BUCKET_NAME}/processed/{table_name}.csv"

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            autodetect=True
        )

        load_job = client.load_table_from_uri(
            gcs_uri,
            table_id,
            job_config=job_config
        )
        load_job.result()  # Wait for the job to complete
        print(f"✅ {table_name} loaded into BigQuery: {table_id}")

if __name__ == "__main__":
    print("🔹 Exporting data from GCS to BigQuery...")
    load_data_to_bigquery()
    print("✅ Data export completed successfully!")