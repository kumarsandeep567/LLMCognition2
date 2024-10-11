
from dotenv import load_dotenv
from google.cloud import storage
from google.oauth2 import service_account
import logging
import pandas as pd
import json
import csv
import re
import os

# Logger function
logging.basicConfig(level = logging.INFO, format = '%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def download_json_from_gcs(bucket_name, blob_name, json_path, creds_file_path):
    logger.info("Inside download_json from Google Cloud Storage function")
    # Creating GCS client

    creds = service_account.Credentials.from_service_account_file(creds_file_path)
    client = storage.Client(credentials = creds)
    bkt = client.bucket(bucket_name)
    logger.info("Connection to Google Cloud Storage successful")

    # Get metadata file
    blob = bkt.blob(blob_name)
    blob.download_to_filename(json_path)
    logger.info(f"Downloaded {blob_name} from GCS bucket {bucket_name} to {json_path}")


def clean_string(value):
    # Remove extra spaces and characters from a string
    logger.info("Inside clean string function")

    if isinstance(value, str):
        value = value.strip()
        value = re.sub(r'[\x00\s\n]', ' ', value)
        value = value.replace('"', '')
        return value if value else ''
    logger.info("Clean string function executed succesfully")
    return str(value)


def clean_data(data):
    # Recursively clean each value in the dictionary
    logger.info("Inside clean data function")

    for key, value in data.items():
        if value is None:
            data[key] = ""
        if isinstance(value, dict):
            data[key] = clean_data(value)
        elif isinstance(value, list):
            data[key] = [clean_string(v) if isinstance(v, str) else str(v) for v in value]
        else:
            data[key] = clean_string(value)
    logger.info("Clean data function executed succesfully")
    return data


def process_json_file(file_path):
    logger.info("Inside processing JSON file function")

    processed_data = []

    # Loads the entire jsonl metadata file
    with open(file_path, 'r') as file:
        for line in file:
            try:
                json_data = json.loads(line)
                cleaned_data = clean_data(json_data)
                processed_data.append(cleaned_data)
            except json.JSONDecodeError as e:
                logger.error(f"Error parsing line: {line}")
                logger.error(f"Error message: {e}")
    logger.info("Processing json file function executed succesfully")
    return processed_data


def load_into_csv(data, csv_filename):
    logger.info("Inside processing JSON file function")
    with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = data[0].keys()
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)
    logger.info("Data loaded into csv file successfully")

def upload_csv_to_gcs(bucket_name, blob_name , csv_filename, creds_file_path):
    logger.info("Uploading csv file into GCS")
    # Upload csv file into GCP
    creds = service_account.Credentials.from_service_account_file(creds_file_path)
    client = storage.Client(credentials = creds)
    bkt = client.bucket(bucket_name)
    blob = bkt.blob(blob_name)
    blob.upload_from_filename(csv_filename)
    print(f"Uploaded {csv_filename} to GCS bucket {bucket_name} as {blob_name}")

def driver_func():
    logger.info("Inside main function")

    # Load Environment variables
    load_dotenv()

    bucket_name = os.getenv("BUCKET_NAME")
    test_blob_name = os.getenv("GCP_FILES_PATH") + os.getenv("TEST_FILE_PATH") + os.getenv("METADATA_FILENAME")
    validation_blob_name = os.getenv("GCP_FILES_PATH") + os.getenv("VALIDATION_FILE_PATH") + os.getenv("METADATA_FILENAME")
    test_json_path = os.getenv("TEST_METADATA_FILENAME") 
    validation_json_path = os.getenv("VALIDATION_METADATA_FILENAME")
    gcp_csv_filepath = os.getenv("GCP_CSV_PATH")
    test_csv_filename = os.getenv("TEST_CSV_FILENAME")
    validation_csv_filename = os.getenv("VALIDATION_CSV_FILENAME")
    creds_file_path = os.getenv("GCS_CREDENTIALS_PATH")

    # Download metadata JSON file from GCS
    download_json_from_gcs(bucket_name, test_blob_name, test_json_path, creds_file_path)
    download_json_from_gcs(bucket_name, validation_blob_name, validation_json_path, creds_file_path)

    # Process the JSON file
    test_processed_data = process_json_file(test_json_path)
    validation_processed_data = process_json_file(validation_json_path)

    # Load processed json data into csv file
    load_into_csv(test_processed_data, test_csv_filename)
    load_into_csv(validation_processed_data, validation_csv_filename)

    # Upload the processed CSV file back to GCS
    upload_csv_to_gcs(bucket_name, f"{gcp_csv_filepath}{test_csv_filename}", test_csv_filename, creds_file_path)
    upload_csv_to_gcs(bucket_name, f"{gcp_csv_filepath}{validation_csv_filename}", validation_csv_filename, creds_file_path)

def main():
    driver_func()

if __name__ == "__main__":
    main()
