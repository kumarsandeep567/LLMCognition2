from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

import os
import io
import re
import csv
import ast
import json
import time
import shutil
import base64
import logging
import pymupdf
import zipfile
import pandas as pd
import mysql.connector
from dotenv import load_dotenv
from unidecode import unidecode
from google.cloud import storage
from mysql.connector import Error
from datetime import datetime
from google.oauth2 import service_account
from azure.core.credentials import AzureKeyCredential
from azure.ai.formrecognizer import DocumentAnalysisClient
from huggingface_hub import login, hf_hub_download, list_repo_files
from adobe.pdfservices.operation.auth.service_principal_credentials import ServicePrincipalCredentials
from adobe.pdfservices.operation.exception.exceptions import ServiceApiException, ServiceUsageException, SdkException
from adobe.pdfservices.operation.io.cloud_asset import CloudAsset
from adobe.pdfservices.operation.io.stream_asset import StreamAsset
from adobe.pdfservices.operation.pdf_services import PDFServices
from adobe.pdfservices.operation.pdf_services_media_type import PDFServicesMediaType
from adobe.pdfservices.operation.pdfjobs.jobs.extract_pdf_job import ExtractPDFJob
from adobe.pdfservices.operation.pdfjobs.params.extract_pdf.extract_element_type import ExtractElementType
from adobe.pdfservices.operation.pdfjobs.params.extract_pdf.table_structure_type import TableStructureType
from adobe.pdfservices.operation.pdfjobs.params.extract_pdf.extract_renditions_element_type import ExtractRenditionsElementType
from adobe.pdfservices.operation.pdfjobs.params.extract_pdf.extract_pdf_params import ExtractPDFParams
from adobe.pdfservices.operation.pdfjobs.result.extract_pdf_result import ExtractPDFResult



# Load the environment variables
load_dotenv()

# Logger function
logging.basicConfig(level = logging.INFO, format = '%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ============================= Logger : Begin =============================

# Initialize logger
pymupdf_logger = logging.getLogger(__name__)
pymupdf_logger.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

# Log to console (dev only)
if os.getenv('APP_ENV', "development") == "development":
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    pymupdf_logger.addHandler(handler)

# Also log to a file
file_handler = logging.FileHandler(os.getenv('PYMUPDF_EXTRACT_LOG_FILE', 'content_extractor_pymupdf.log'))
file_handler.setFormatter(formatter)
pymupdf_logger.addHandler(file_handler) 

# ============================= Logger : End ===============================


def pdf_downloader() -> bool:
    '''Download PDF files from a HuggingFace repository and save them locally in respective directories'''

    # Set the parameters 
    access_token        = os.getenv("HUGGINGFACE_TOKEN", None)
    repository_id       = os.getenv("REPO_ID", None)
    repository_type     = os.getenv("REPO_TYPE", None)
    hf_directory_path   = os.getenv("DIRECTORY_PATH", None)

    pymupdf_logger.info("AIRFLOW - pdf_downloader() - Request to download PDF files received")
    status = False

    # Validate that all required inputs are provided
    if access_token is None:
        pymupdf_logger.error("AIRFLOW - pdf_downloader() - HuggingFace token was expected but received None")
        return status

    if repository_id is None:
        pymupdf_logger.error("AIRFLOW - pdf_downloader() - HuggingFace Repository ID was expected but received None")
        return status

    if repository_type is None:
        pymupdf_logger.error("AIRFLOW - pdf_downloader() - HuggingFace Repository Type was expected but received None")
        return status

    if hf_directory_path is None:
        pymupdf_logger.error("AIRFLOW - pdf_downloader() - Directory path for HuggingFace Repository was expected but received None")
        return status

    try:

        # List files from the HuggingFace repository
        pymupdf_logger.info("AIRFLOW - pdf_downloader() - Fetching file list from HuggingFace")
        dataset_file_list = list_repo_files(
            token       = access_token,
            repo_id     = repository_id,
            repo_type   = repository_type
        )
        pymupdf_logger.info("AIRFLOW - pdf_downloader() - File list fetched from HuggingFace")

        if len(dataset_file_list) > 0:
            try:
                # Remove the directories if they exist
                if os.path.isdir('2023'):
                    shutil.rmtree('2023')
            
            except OSError as exception:
                pymupdf_logger.error("AIRFLOW - pdf_downloader() - Error removing directories")
                pymupdf_logger.error(exception)
                return status

            # Download PDF files and save them in appropriate directories
            pymupdf_logger.info("AIRFLOW - pdf_downloader() - Downloading PDF files from HuggingFace")
            for file in dataset_file_list:
                if file.endswith('.pdf'):
                    try:

                        # Download the PDF file from the repository
                        hf_hub_download(
                            token      = access_token,
                            repo_id    = repository_id,
                            repo_type  = repository_type,
                            filename   = file,
                            local_dir = "./"
                        )

                    except Exception as exception:
                        pymupdf_logger.error(f"AIRFLOW - pdf_downloader() - Error downloading file {file}")
                        pymupdf_logger.error(exception)

            # Everything worked hopefully
            status = True
            pymupdf_logger.info("AIRFLOW - pdf_downloader() - PDF files downloaded successfully")
        
        else:
            pymupdf_logger.error("AIRFLOW - pdf_downloader() - Zero files were found in the repository. Are the repository details correct?")
    
    except Exception as exception:
        pymupdf_logger.error("AIRFLOW - pdf_downloader() - Error accessing HuggingFace repository")
        pymupdf_logger.error(exception)
    
    return status


def get_pdf_list():
    '''Create a list of PDFs (with absolute paths) to parse'''

    pymupdf_logger.info("AIRFLOW - get_pdf_list() - Request received to create a list of PDF files")
    pdf_list = []
    
    try:
        # Get the list of PDFs from their directories
        test_dir_path = os.path.join(os.getcwd(), '2023', 'test')
        test_dir_pdfs = os.listdir(test_dir_path)

        for _file in test_dir_pdfs:
            pdf_list.append(os.path.join(test_dir_path, _file))
        
        validation_dir_path = os.path.join(os.getcwd(), '2023', 'validation')
        validation_dir_pdfs = os.listdir(validation_dir_path)

        for _file in validation_dir_pdfs:
            pdf_list.append(os.path.join(validation_dir_path, _file))

        pymupdf_logger.info("AIRFLOW - get_pdf_list() - Creation of a list of PDF files completed")

    except Exception as exception:
        pdf_list = None
        pymupdf_logger.error("AIRFLOW - get_pdf_list() - Error fetching directory contents")
        pymupdf_logger.error(exception)
    
    return pdf_list

def load_files_into_gcp(repository_id, repository_type, files, bucket_name, creds_file_path, gcp_folder_path):
    logger.info("Airflow - fileLoader_driver_func() - load_files_into_gcp() - Loading files into Google Cloud Storage bucket")

    try:
        # Creating Google Cloud Storage Client
        creds = service_account.Credentials.from_service_account_file(creds_file_path)
        client = storage.Client(credentials = creds)
        bkt = client.bucket(bucket_name)

        logger.info("Airflow - fileLoader_driver_func() - load_files_into_gcp() - Connection to GCS successful")

        for file in files:
            file_data = hf_hub_download(
                repo_id = repository_id,
                repo_type = repository_type,
                filename = file
            )
            
            # Checking if the file is from test/validation set
            if "test" in file:
                gcs_file_path = os.path.join(gcp_folder_path, "test", os.path.basename(file))
            elif "validation" in file:
                gcs_file_path = os.path.join(gcp_folder_path, "validation", os.path.basename(file))
            else:
                logger.warning(f"Airflow - fileLoader_driver_func() - load_files_into_gcp() - File {file} does not match test or validation categories. Skipping upload.")
                continue

            # Upload to GCS from request response
            blob = bkt.blob(gcs_file_path)
            blob.upload_from_filename(file_data)
            logger.info(f"Airflow - fileLoader_driver_func() - load_files_into_gcp() - Uploaded {file} to GCS bucket {bucket_name}")
            logger.info("Airflow - fileLoader_driver_func() - load_files_into_gcp() - Files successfully loaded to Google Cloud Storage")

    except Exception as e:
        logger.error("Airflow - fileLoader_driver_func() - load_files_into_gcp() - GCP connection failed")
        raise e


def load_files(access_token, repository_id, repository_type, file_path):
    logger.info("Airflow - fileLoader_driver_func() - load_files() - Loading files from hugging face")

    # Login to hugging face
    login(token = access_token, add_to_git_credential = False)

    # Load all the files from the GAIA benchmark repository
    files = list_repo_files(
        repo_id = repository_id,
        repo_type = repository_type
    )

    # Filter validation files
    total_files = [file for file in files if file.startswith(file_path)]
    logger.info("Airflow - fileLoader_driver_func() - load_files() - Files successfully downloaded from Hugging Face")
    return total_files

def fileLoader_driver_func():

    logger.info("Airflow - fileLoader_driver_func() - Loading files from Hugging Face to GCS")

    access_token = os.getenv("HUGGINGFACE_TOKEN")
    repository_id = os.getenv("REPO_ID")
    repository_type = os.getenv("REPO_TYPE")
    bucket_name = os.getenv("BUCKET_NAME")
    creds_file_path = os.getenv("GCS_CREDENTIALS_PATH")
    file_path = os.getenv("FILE_PATH")
    gcp_folder_path = os.getenv("GCP_FILES_PATH")

    # Call load_files function to download all the files from validation set
    files = load_files(access_token, repository_id, repository_type, file_path)
    load_files_into_gcp(repository_id, repository_type, files, bucket_name, creds_file_path, gcp_folder_path)
    logger.info("Airflow - fileLoader_driver_func() - Loading files from Hugging Face to GCS executed successfully")


def download_json_from_gcs(bucket_name, blob_name, json_path, creds_file_path):
    logger.info("Airflow - fileParser_driver_func() - download_json_from_gcs() - Inside download_json from Google Cloud Storage function")
    # Creating GCS client

    creds = service_account.Credentials.from_service_account_file(creds_file_path)
    client = storage.Client(credentials = creds)
    bkt = client.bucket(bucket_name)
    logger.info("Airflow - fileParser_driver_func() - download_json_from_gcs() - Connection to Google Cloud Storage successful")

    # Get metadata file
    blob = bkt.blob(blob_name)
    blob.download_to_filename(json_path)
    logger.info(f"Airflow - fileParser_driver_func() - download_json_from_gcs() - Downloaded {blob_name} from GCS bucket {bucket_name} to {json_path}")


def clean_string(value):
    # Remove extra spaces and characters from a string
    logger.info("Airflow - fileParser_driver_func() - clean_string() - Inside clean string function")

    if isinstance(value, str):
        value = value.strip()
        value = re.sub(r'[\x00\s\n]', ' ', value)
        value = value.replace('"', '')
        return value if value else ''
    logger.info("Airflow - fileParser_driver_func() - clean_string() - Clean string function executed succesfully")
    return str(value)


def clean_data(data):
    # Recursively clean each value in the dictionary
    logger.info("Airflow - fileParser_driver_func() - clean_data() - Inside clean data function")

    for key, value in data.items():
        if value is None:
            data[key] = ""
        if isinstance(value, dict):
            data[key] = clean_data(value)
        elif isinstance(value, list):
            data[key] = [clean_string(v) if isinstance(v, str) else str(v) for v in value]
        else:
            data[key] = clean_string(value)
    logger.info("Airflow - fileParser_driver_func() - clean_data() - Clean data function executed succesfully")
    return data


def process_json_file(file_path):
    logger.info("Airflow - fileParser_driver_func() - process_json_file() - Inside processing JSON file function")

    processed_data = []

    # Loads the entire jsonl metadata file
    with open(file_path, 'r') as file:
        for line in file:
            try:
                json_data = json.loads(line)
                cleaned_data = clean_data(json_data)
                processed_data.append(cleaned_data)
            except json.JSONDecodeError as e:
                logger.error(f"Airflow - fileParser_driver_func() - process_json_file() - Error parsing line: {line}")
                logger.error(f"Airflow - fileParser_driver_func() - process_json_file() - Error message: {e}")
    logger.info("Airflow - fileParser_driver_func() - process_json_file() - Processing json file function executed succesfully")
    return processed_data


def load_into_csv(data, csv_filename):
    logger.info("Airflow - fileParser_driver_func() - load_into_csv() - Inside processing JSON file function")
    with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = data[0].keys()
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)
    logger.info("Airflow - fileParser_driver_func() - load_into_csv() - Data loaded into csv file successfully")

def upload_csv_to_gcs(bucket_name, blob_name , csv_filename, creds_file_path):
    logger.info("Airflow - fileParser_driver_func() - upload_csv_to_gcs() - Uploading csv file into GCS")
    # Upload csv file into GCP
    creds = service_account.Credentials.from_service_account_file(creds_file_path)
    client = storage.Client(credentials = creds)
    bkt = client.bucket(bucket_name)
    blob = bkt.blob(blob_name)
    blob.upload_from_filename(csv_filename)
    print(f"Airflow - fileParser_driver_func() - upload_csv_to_gcs() - Uploaded {csv_filename} to GCS bucket {bucket_name} as {blob_name}")

def fileParser_driver_func():
    logger.info("Airflow - fileParser_driver_func() - Parsing Metadata file and loading it into CSV")

    # Load Environment variables
    load_dotenv()

    bucket_name = os.getenv("BUCKET_NAME")
    test_blob_name = os.path.join(os.getenv("GCP_FILES_PATH"), os.getenv("TEST_FILE_PATH"), os.getenv("METADATA_FILENAME"))
    validation_blob_name = os.path.join(os.getenv("GCP_FILES_PATH"), os.getenv("VALIDATION_FILE_PATH"), os.getenv("METADATA_FILENAME"))
    test_json_path = os.getenv("TEST_METADATA_FILENAME") 
    validation_json_path = os.getenv("VALIDATION_METADATA_FILENAME")
    gcp_csv_filepath = os.getenv("GCP_CSV_PATH")
    test_csv_filename = os.getenv("TEST_CSV_FILENAME")
    validation_csv_filename = os.getenv("VALIDATION_CSV_FILENAME")
    creds_file_path = os.getenv("GCS_CREDENTIALS_PATH")

    # Download metadata JSON file from GCS
    download_json_from_gcs(bucket_name, test_blob_name, test_json_path, creds_file_path)
    download_json_from_gcs(bucket_name, validation_blob_name, validation_json_path, creds_file_path)
    logger.info("Airflow - fileParser_driver_func() - JSON files downloaded from GCS")

    # Process the JSON file
    test_processed_data = process_json_file(test_json_path)
    validation_processed_data = process_json_file(validation_json_path)
    logger.info("Airflow - fileParser_driver_func() - JSON files processed")

    # Load processed json data into csv file
    load_into_csv(test_processed_data, test_csv_filename)
    load_into_csv(validation_processed_data, validation_csv_filename)
    logger.info("Airflow - fileParser_driver_func() - Processed JSON files are loded into CSV")

    # Upload the processed CSV file back to GCS
    upload_csv_to_gcs(bucket_name, f"{gcp_csv_filepath}{test_csv_filename}", test_csv_filename, creds_file_path)
    upload_csv_to_gcs(bucket_name, f"{gcp_csv_filepath}{validation_csv_filename}", validation_csv_filename, creds_file_path)
    logger.info("Airflow - fileParser_driver_func() - Processed CSV files uploaded to GCS csv_files_path")

    logger.info("Airflow - fileParser_driver_func() - Parsing Metadata file and loading it into CSV executed successfully")

def download_pdf_from_gcs(bucket_name, file_name, creds_file_path):
    logger.info("Airflow - azure_pdfFileExtractor_driver_func.py() - download_pdf_from_gcs() - Downloading all pdf files from GCS")
    creds = service_account.Credentials.from_service_account_file(creds_file_path)
    client = storage.Client(credentials=creds)
    blob = client.bucket(bucket_name).blob(file_name)

    logger.info(f"Airflow - azure_pdfFileExtractor_driver_func.py() - download_pdf_from_gcs() - Downloading: {file_name}")
    pdf_bytes = blob.download_as_bytes()  # Downloading as bytes
    return pdf_bytes

def download_pdf_files(bucket_name, gcp_files_path, folder_name, creds_file_path):
    logger.info("Airflow - azure_pdfFileExtractor_driver_func.py() - download_pdf_files() - Downloading all pdf files from GCS")
    creds = service_account.Credentials.from_service_account_file(creds_file_path)
    client = storage.Client(credentials=creds)
    bkt = client.bucket(bucket_name)

    blobs = bkt.list_blobs(prefix=os.path.join(gcp_files_path, folder_name))
    pdf_data_list = []
    pdf_file_names = []

    for blob in blobs:
        if blob.name.endswith('.pdf'):
            pdf_data = download_pdf_from_gcs(bucket_name, blob.name, creds_file_path)
            pdf_data_list.append(pdf_data)
            pdf_file_names.append(os.path.basename(blob.name))

    logger.info("Airflow - azure_pdfFileExtractor_driver_func.py() - download_pdf_files() - Downloading all pdf files from GCS executed successfully")

    return pdf_data_list, pdf_file_names

def extract_data_from_pdf(pdf_data, endpoint, key):
    logger.info("Airflow - azure_pdfFileExtractor_driver_func.py() - extract_data_from_pdf() - Extracting data with Azure Document Analysis Client")
    client = DocumentAnalysisClient(endpoint=endpoint, credential=AzureKeyCredential(key))

    logger.info("Airflow - azure_pdfFileExtractor_driver_func.py() - extract_data_from_pdf() - Sending PDF for analysis")
    poller = client.begin_analyze_document("prebuilt-document", document=io.BytesIO(pdf_data))
    result = poller.result()

    # Initialize containers for extracted data
    extracted_data = {
        "tables": [],
        "text": [],
        "images": []
    }

    logger.info("Airflow - azure_pdfFileExtractor_driver_func.py() - extract_data_from_pdf() - Extracting data from PDF")
    for page in result.pages:
        page_data = {
            "page_number": page.page_number,
            "tables": [],
            "text": "",
            "images": []
        }

        # Extract tables
        if hasattr(page, 'tables') and page.tables:
            for table in page.tables:
                table_data = []
                for cell in table.cells:
                    table_data.append({
                        "row_index": cell.row_index,
                        "column_index": cell.column_index,
                        "text": cell.content
                    })
                page_data['tables'].append(table_data)

        # Extract text
        page_data['text'] = ' '.join([line.content for line in page.lines if line.content.strip()])

        # Extract images
        if hasattr(page, 'images') and page.images:
            for image in page.images:
                page_data['images'].append({
                    "image_content": image.content,
                    "page_number": page.page_number
                })

        extracted_data["tables"].extend(page_data["tables"])
        extracted_data["text"].append(page_data["text"])
        extracted_data["images"].extend(page_data["images"])
        logger.info("Airflow - azure_pdfFileExtractor_driver_func.py() - extract_data_from_pdf() - Extracting data with Azure AI Document Intelligence Tool executed successfully")

    return extracted_data

def save_data(gcs_extract_file_path, folder_name, extracted_data, pdf_file_name):
    logger.info("Airflow - azure_pdfFileExtractor_driver_func.py() - save_data() - Saving Extracted files to Local Directory")
    pdf_folder = os.path.join(gcs_extract_file_path, folder_name, os.path.splitext(pdf_file_name)[0])
    os.makedirs(pdf_folder, exist_ok=True)

    json_pdf_folder = os.path.join(pdf_folder, "JSON")
    csv_pdf_folder = os.path.join(pdf_folder, "CSV")
    image_pdf_folder = os.path.join(pdf_folder, "Images")

    os.makedirs(json_pdf_folder, exist_ok=True)
    os.makedirs(csv_pdf_folder, exist_ok=True)
    os.makedirs(image_pdf_folder, exist_ok=True)

    # Save tables to CSV
    for i, table in enumerate(extracted_data['tables']):
        df = pd.DataFrame(table)
        csv_file_path = os.path.join(csv_pdf_folder, f'table_{i}.csv')
        df.to_csv(csv_file_path, index=False)
        logger.info(f"Airflow - azure_pdfFileExtractor_driver_func.py() - save_data() - Saved table {i} to {csv_file_path}.")

    # Save text to JSON
    for page_number, text in enumerate(extracted_data['text'], 1):
        json_file_path = os.path.join(json_pdf_folder, f'page_{page_number}.json')
        with open(json_file_path, 'w') as json_file:
            json.dump({"page_number": page_number, "text": text}, json_file)
        logger.info(f"Airflow - azure_pdfFileExtractor_driver_func.py() - save_data() - Saved text for page {page_number} to {json_file_path}.")

    # Save images
    for i, image in enumerate(extracted_data['images']):
        image_file_path = os.path.join(image_pdf_folder, f'image_page_{image["page_number"]}_{i}.png')
        if image['image_content'].startswith('data:image/png;base64,'):
            _, encoded = image['image_content'].split(',', 1)
            image_data = base64.b64decode(encoded)
            with open(image_file_path, 'wb') as img_file:
                img_file.write(image_data)
            logger.info(f"Airflow - azure_pdfFileExtractor_driver_func.py() - save_data() - Saved image from page {image['page_number']} to {image_file_path}.")
        else:
            logger.warning(f"Airflow - azure_pdfFileExtractor_driver_func.py() - save_data() - Unsupported image format on page {image['page_number']}.")

    logger.info("Airflow - azure_pdfFileExtractor_driver_func.py() - save_data() - Saving Extracted files to Local Directory executed successfully")

def azure_pdfFileExtractor_driver_func():
    logger.info("Airflow - azure_pdfFileExtractor_driver_func.py() - Text extracted from pdf files by Azure AI Document Intelligence Tool")

    # Load env variables
    load_dotenv()

    bucket_name = os.getenv("BUCKET_NAME")
    gcp_files_path = os.getenv("GCP_FILES_PATH")
    test_files_path = os.getenv("TEST_FILE_PATH")
    validation_files_path = os.getenv("VALIDATION_FILE_PATH")
    creds_file_path = os.getenv("GCS_CREDENTIALS_PATH")
    azure_endpoint = os.getenv("AZURE_ENDPOINT")
    azure_key = os.getenv("AZURE_KEY")
    azure_filepath = os.getenv("GCS_AZURE_FILEPATH")


    # Download PDFs
    for folder_name in [test_files_path, validation_files_path]:
        pdf_data_list, pdf_file_names = download_pdf_files(bucket_name, gcp_files_path, folder_name, creds_file_path)
        logger.info("Airflow - azure_pdfFileExtractor_driver_func.py() - Extracting pdf files from {folder_name}")

        # Extract data from downloaded PDFs
        for pdf_data, pdf_file_name in zip(pdf_data_list, pdf_file_names):
            logger.info("Airflow - azure_pdfFileExtractor_driver_func.py() - Extracting data from {pdf_file_name} from {folder_name}")
            extracted_data = extract_data_from_pdf(pdf_data, azure_endpoint, azure_key)
            save_data(azure_filepath, folder_name, extracted_data, pdf_file_name)

    logger.info("Airflow - azure_pdfFileExtractor_driver_func.py() - Text extracted successfully from pdf files by Azure AI Document Intelligence Tool")


def extract_content_pymupdf():
    '''Extract the contents of the PDF and store the contents in JSON and CSV formats, wherever needed'''

    pymupdf_logger.info("AIRFLOW - extract_content_pymupdf() - Request received to extract PDF contents through PyMuPDF")

    try:
        # Remove the directory if it exists
        if os.path.isdir('extracted_contents'):
            shutil.rmtree('extracted_contents')

    except OSError as exception:
        pymupdf_logger.error("AIRFLOW - extract_content_pymupdf() - Error removing directories")
        pymupdf_logger.error(exception)

    # Get the list of PDFs
    pdf_list = get_pdf_list()
    
    if pdf_list is not None:

        # Iterate through the list of PDF documents
        for pdf_path in pdf_list:
            try:
                with pymupdf.open(pdf_path) as document:
                    
                    pdf_name = os.path.splitext(os.path.basename(pdf_path))[0]
                    
                    # Create the directory for the PDF
                    base_dir = os.path.join(os.getcwd(), 'extracted_contents', pdf_name)
                    os.makedirs(base_dir)
                    
                    # Sub-directories for storing JSON, images, and tables
                    json_dir = os.path.join(base_dir, 'JSON')
                    img_dir = os.path.join(base_dir, 'Image')
                    csv_dir = os.path.join(base_dir, 'CSV')
                    
                    os.makedirs(json_dir)
                    os.makedirs(img_dir)
                    os.makedirs(csv_dir)
                    
                    # Loop through each page and extract content
                    for page_num in range(document.page_count):
                        try:
                            page = document[page_num]
                            page_id = page_num + 1
                            
                            # Create a dictionary to store the page content
                            page_content = {
                                "page_id": page_id,
                                "content": {}
                            }
                            
                            # Extract text content
                            text = page.get_text("text")
                            page_content['content']['text'] = unidecode(text)
                            
                            # Extract images
                            image_list = []
                            images = page.get_images(full=True)
                            
                            for img_index, img in enumerate(images):
                                try:
                                    xref = img[0]
                                    img_data = document.extract_image(xref)
                                    img_ext = img_data["ext"]
                                    img_name = f"{page_id}_image_{img_index}.{img_ext}"
                                    img_path = os.path.join(img_dir, img_name)

                                    with open(img_path, 'wb') as img_file:
                                        img_file.write(img_data["image"])

                                    image_list.append(img_name)
                                except Exception as exception:
                                    pymupdf_logger.error(f"Error extracting image on Page {page_id} of PDF {pdf_path}")
                                    pymupdf_logger.error(exception)
                            
                            page_content['content']['image'] = image_list
                            
                            # Extract tables
                            table_list = []
                            tables = page.find_tables()
                            
                            for table_index, table in enumerate(tables):
                                try:
                                    table_data = table.extract()

                                    # Convert to dataframe
                                    table_df = pd.DataFrame(table_data[1:], columns=table_data[0])

                                    # Write to CSV file
                                    table_name = f"{page_id}_table_{table_index}.csv"
                                    table_path = os.path.join(csv_dir, table_name)
                                    table_df.to_csv(table_path, index=False)
                                    table_list.append(table_name)
                                
                                except Exception as exception:
                                    pymupdf_logger.error(f"Error extracting table on Page {page_id} of PDF {pdf_path}")
                                    pymupdf_logger.error(exception)

                        
                            page_content['content']['table'] = table_list
                            
                            # Save page content as JSON
                            json_file_path = os.path.join(json_dir, f"{page_id}.json")
                            with open(json_file_path, 'w') as json_file:
                                json.dump(page_content, json_file, indent=4)
                        
                        except Exception as exception:
                            pymupdf_logger.error(f"AIRFLOW - extract_content_pymupdf() - Error occured while processing Page {page_id+1} of PDF {pdf_path}")
                            pymupdf_logger.error(exception)

            except Exception as exception:
                pymupdf_logger.error("AIRFLOW - extract_content_pymupdf() - Failed to open the PDF document")
                pymupdf_logger.error(exception)

        pymupdf_logger.info("AIRFLOW - get_pdf_list() - Content extraction through PyMuPDF complete")


def extract_metadata():
    """Extracts metadata including word count, image count, and table count for each PDF."""

    pymupdf_logger.info("AIRFLOW - extract_metadata() - Request received to create metadata file for PDFs through PyMuPDF")

    pdf_list = get_pdf_list()

    for pdf_path in pdf_list:
        pdf_name = os.path.splitext(os.path.basename(pdf_path))[0]
        
        try:
            with pymupdf.open(pdf_path) as document:
                
                # Get basic metadata
                metadata = document.metadata
                metadata["number_of_pages"] = document.page_count
                
                # Initialize counters
                word_count = 0
                image_count = 0
                table_count = 0
                
                # Navigate to the JSON folder
                json_dir = os.path.join(os.getcwd(), 'extracted_contents', pdf_name, 'JSON')
                
                # Throw an exception if the JSON folder is missing
                if not os.path.exists(json_dir):
                    pymupdf_logger.error(f"JSON directory for {pdf_name} not found")
                    raise FileNotFoundError(f"JSON directory for {pdf_name} not found")
                
                # Iterate through JSON files (one for each page) and count words, images, and tables
                for json_file in os.listdir(json_dir):
                    json_file_path = os.path.join(json_dir, json_file)
                    
                    with open(json_file_path, 'r') as file:
                        page_content = json.load(file)
                        
                        text_content = page_content["content"].get("text", "")
                        word_count += len(text_content.split())
                        
                        image_content = page_content["content"].get("image", [])
                        image_count += len(image_content)
                        
                        table_content = page_content["content"].get("table", [])
                        table_count += len(table_content)
                
                # Update metadata with new key-values
                metadata["number_of_words"] = word_count
                metadata["number_of_images"] = image_count
                metadata["number_of_tables"] = table_count
                
                # Write metadata to metadata.json in the PDF's directory
                base_dir = os.path.join(os.getcwd(), 'extracted_contents', pdf_name)
                metadata_file = os.path.join(base_dir, 'metadata.json')
                
                with open(metadata_file, 'w') as metadata_output:
                    json.dump(metadata, metadata_output, indent=4)
        
        except Exception as exception:
            pymupdf_logger.error(f"AIRFLOW - extract_metadata() - Error occured while processing PDF {pdf_name}")
            pymupdf_logger.error(exception)

    pymupdf_logger.info("AIRFLOW - extract_metadata() - Metadata extraction through PyMuPDF complete")


def create_connection(attempts = 3, delay = 2):
    '''Start a connection with the MySQL database'''

    # Database connection config
    config = {
        'user'              : os.getenv('MYSQL_USER'),
        'password'          : os.getenv('MYSQL_PASSWORD'),
        'host'              : os.getenv('MYSQL_HOST'),
        'database'          : os.getenv('DB_NAME'),
        'raise_on_warnings' : False
    }

    # Attempt a reconnection routine
    attempt = 1
    
    while attempt <= attempts:
        try:
            conn = mysql.connector.connect(**config)
            pymupdf_logger.info("DATABASE - create_connection() - Connection to the database was opened")
            return conn
        
        except (Error, IOError) as error:
            if attempt == attempts:
                # Ran out of attempts
                pymupdf_logger.error(f"DATABASE - create_connection() - Failed to connect to database : {error}")
                return None
            else:
                pymupdf_logger.warning(f"DATABASE - create_connection() - Connection failed: {error} - Retrying {attempt}/{attempts} ...")
                
                # Delay the next attempt
                time.sleep(delay ** attempt)
                attempt += 1
    
    return None


def setup_tables() -> None:
    '''Drop existing tables, if any, and create the tables in the database'''

    pymupdf_logger.info("DATABASE - setup_tables() - Request to setup tables received")

    # Compile a list of queries to execute
    queries = {
        "drop_tables": {
            "drop_analytics_table"                  : "DROP TABLE IF EXISTS analytics;",
            "drop_annotation_table"                 : "DROP TABLE IF EXISTS gaia_annotations;",
            "drop_features_table"                   : "DROP TABLE IF EXISTS gaia_features;",
            "drop_users_table"                      : "DROP TABLE IF EXISTS users;",
            "drop_pymupdf_attachment_mapping_table" : "DROP TABLE IF EXISTS pymupdf_attachment_mapping;",
            "drop_pymupdf_page_info_table"          : "DROP TABLE IF EXISTS pymupdf_page_info;",
            "drop_pymupdf_attachment_table"         : "DROP TABLE IF EXISTS pymupdf_attachments;",
            "drop_pymupdf_info_table"               : "DROP TABLE IF EXISTS pymupdf_info;",
            "drop_adobe_info_table"                 : "DROP TABLE IF EXISTS adobe_info;",
            "drop_azure_info_table"                 : "DROP TABLE IF EXISTS azure_info;",
        },
        "create_tables": {
            "create_features_table": """
                CREATE TABLE gaia_features(
                    task_id VARCHAR(255) PRIMARY KEY,
                    dataset_type VARCHAR(255) NOT NULL,
                    question TEXT,
                    level INT,
                    final_answer VARCHAR(255),
                    file_name VARCHAR(255),
                    file_path VARCHAR(255)
                );
            """,
            "create_annotation_table": """
                CREATE TABLE gaia_annotations(
                    task_id VARCHAR(255) PRIMARY KEY,
                    steps TEXT,
                    number_of_steps VARCHAR(255),
                    time_taken VARCHAR(255),
                    tools TEXT,
                    number_of_tools VARCHAR(255),
                    FOREIGN KEY (task_id) REFERENCES gaia_features(task_id)
                );
            """,
            "create_users_table": """
                CREATE TABLE IF NOT EXISTS users(
                    user_id INT PRIMARY KEY AUTO_INCREMENT,
                    first_name VARCHAR(50) NOT NULL,
                    last_name VARCHAR(50) NOT NULL,
                    phone VARCHAR(15) NOT NULL,
                    email VARCHAR(100) NOT NULL,
                    password VARCHAR(255) NOT NULL,
                    jwt_token TEXT DEFAULT NULL
                );
            """,
            "create_pymupdf_info_table": """
                CREATE TABLE pymupdf_info(
                    pdf_id INT PRIMARY KEY AUTO_INCREMENT,
                    file_name VARCHAR(255) NOT NULL,
                    title VARCHAR(255) DEFAULT NULL,
                    format VARCHAR(255),
                    creator VARCHAR(255) DEFAULT NULL,
                    author VARCHAR(255) DEFAULT NULL,
                    encryption VARCHAR(20) DEFAULT NULL,
                    number_of_pages INT,
                    number_of_words INT,
                    number_of_images INT,
                    number_of_tables INT
                );
            """,
            "create_pymupdf_page_info_table": """
                CREATE TABLE pymupdf_page_info(
                    info_id INT PRIMARY KEY AUTO_INCREMENT,
                    page_id INT NOT NULL,
                    pdf_id INT NOT NULL,
                    text TEXT DEFAULT NULL,
                    FOREIGN KEY (pdf_id) REFERENCES pymupdf_info(pdf_id),
                    INDEX (page_id)
                );
            """,
            "create_pymupdf_attachment_table": """
                CREATE TABLE pymupdf_attachments(
                    attachment_id INT PRIMARY KEY AUTO_INCREMENT,
                    attachment_name VARCHAR(255) NOT NULL,
                    attachment_url TEXT NOT NULL
                );
            """,
            "create_pymupdf_attachment_mapping_table": """
                CREATE TABLE pymupdf_attachment_mapping(
                    mapping_id INT PRIMARY KEY AUTO_INCREMENT,
                    pdf_id INT NOT NULL,
                    page_id INT NOT NULL,
                    attachment_id INT NOT NULL,
                    FOREIGN KEY (pdf_id) REFERENCES pymupdf_info(pdf_id),
                    FOREIGN KEY (page_id) REFERENCES pymupdf_page_info(page_id),
                    FOREIGN KEY (attachment_id) REFERENCES pymupdf_attachments(attachment_id)
                );
            """,
            "create_adobe_info_table": """
                CREATE TABLE IF NOT EXISTS adobe_info(
                info_id INT PRIMARY KEY AUTO_INCREMENT,
                text TEXT DEFAULT NULL,
                page_id INT NOT NULL,
                is_encrypted TINYINT(1) DEFAULT 0,
                number_of_pages INT NOT NULL,
                pdf_filename VARCHAR(255) NOT NULL
                );
            """,
            "create_azure_info_table": """
                CREATE TABLE azure_info(
                    info_id INT PRIMARY KEY AUTO_INCREMENT,
                    page_id INT NOT NULL,
                    text TEXT DEFAULT NULL,
                    pdf_filename VARCHAR(255) NOT NULL

                );
            """,
            "create_analytics_table": """
                CREATE TABLE IF NOT EXISTS analytics(
                    id INT PRIMARY KEY AUTO_INCREMENT,
                    user_id INT NOT NULL,
                    task_id VARCHAR(255) NOT NULL,
                    updated_steps TEXT DEFAULT NULL,
                    tokens_per_text_prompt VARCHAR(255) DEFAULT NULL,
                    tokens_per_attachment VARCHAR(255) DEFAULT NULL,
                    gpt_response TEXT DEFAULT NULL,
                    total_cost DOUBLE DEFAULT NULL,
                    time_consumed VARCHAR(255) DEFAULT NULL,
                    feedback TEXT NULL,
                    time_stamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    extraction_service varchar(50) DEFAULT NULL,
                    marked_correct int(11) DEFAULT NULL,
                    FOREIGN KEY (user_id) REFERENCES users(user_id),
                    FOREIGN KEY (task_id) REFERENCES gaia_features(task_id)
                );
            """
        }
    }
    
    try:
        # Setup a connection to the database
        conn = create_connection()

        if conn and conn.is_connected():
            cursor = conn.cursor()

            # Start the transaction
            for _, tables in queries.items():
                for table_name, query in tables.items():
                    pymupdf_logger.info(f"SQL - setup_tables() - Running query for {table_name}")

                    cursor.execute(query)
                    conn.commit()

                    pymupdf_logger.info(f"SQL - setup_tables() - Query executed successfully for {table_name}")
    
    except Exception as exception:
        pymupdf_logger.error("DATABASE - setup_tables() - Error occured while setting up the tables")
        pymupdf_logger.error(exception)

    finally:
        if conn and conn.is_connected():
            conn.close()
            pymupdf_logger.info("DATABASE - setup_tables() - Connection to the database was closed")


def download_csv_from_gcs(bucket_name, blob_name, local_file_path, creds_file_path):
    # Download CSV file from GCS
    logger.info("SQL - download_csv_from_gcs() - In function to download CSV file from GCS")

    creds = service_account.Credentials.from_service_account_file(creds_file_path)
    client = storage.Client(credentials = creds)
    bkt = client.bucket(bucket_name)
    blob = bkt.blob(blob_name)
    blob.download_to_filename(local_file_path)
    logger.info(f"SQL - download_csv_from_gcs() - Downloaded {blob_name} from GCS bucket {bucket_name} to {local_file_path}")

def get_file_paths(bucket_name, creds_file_path, gcp_folder_path):
    logger.info("SQL - get_file_paths() - Retrieving file paths from GCS")
    # Retrieve file names from GCS bucket
    storage_client = storage.Client.from_service_account_json(creds_file_path)
    blobs = storage_client.list_blobs(bucket_name, prefix = gcp_folder_path)
    file_path_dict = {os.path.basename(blob.name): f"/{bucket_name}/{blob.name}" for blob in blobs if blob.name.startswith(gcp_folder_path)}
    return file_path_dict

def format_csv_data(df, file_paths_dict, dataset_type):
    logger.info("SQL - format_csv_data() - Formatting data inside CSV file")
    formatted_data = []
    formatted_metadata = []
    
    for index, row in df.iterrows():
        file_name = None if ((pd.isna(row['file_name'])) or (row['file_name'] == '')) else row['file_name'].strip('"')
        file_path = file_paths_dict.get(file_name)

        formatted_row = {
            'task_id': row["task_id"].strip('"'),
            'dataset_type': dataset_type,
            'question': row["Question"].strip('"'),
            'level': int(row["Level"]),
            'final_answer': row['Final answer'].strip('"'),
            'file_name': file_name,
            'file_path': file_path
        }
        formatted_data.append(formatted_row)

        metadata_str = row['Annotator Metadata']
        metadata = ast.literal_eval(metadata_str)

        # Replace final_answer in steps with an empty string
        if 'final_answer' in formatted_row:
            final_answer = formatted_row['final_answer']
            metadata['Steps'] = metadata['Steps'].replace(final_answer, '') 


        formatted_metadata_row = {
            'task_id': row['task_id'].strip('"'),
            'steps' : metadata['Steps'],
            'number_of_steps' : metadata['Number of steps'],
            'time_taken' : metadata['How long did this take?'],
            'tools' : metadata['Tools'],
            'number_of_tools' : metadata['Number of tools']
        }
        formatted_metadata.append(formatted_metadata_row)
    logger.info("SQL - format_csv_data() - Features and metadata formatting done")
    return formatted_data, formatted_metadata

def loadDatabase_driver_func():
    logger.info("cInside load_parsed_data_to_db() function")
    logger.info("SQL - load_parsed_data_to_db() - Uploading parsed test and validation file data into gaia_features, gaia_annotations table to the Database")

    # Environment variables
    bucket_name = os.getenv("BUCKET_NAME")
    creds_file_path = os.getenv("GCS_CREDENTIALS_PATH")
    gcp_files_path = os.getenv("GCP_FILES_PATH")
    test_files_path = os.getenv("TEST_FILE_PATH")
    validation_files_path = os.getenv("VALIDATION_FILE_PATH")
    test_blob_name = os.getenv("GCP_CSV_PATH") + os.getenv("TEST_CSV_FILENAME")
    validation_blob_name = os.getenv("GCP_CSV_PATH") + os.getenv("VALIDATION_CSV_FILENAME")
    local_test_csv_path = os.getenv("TEST_CSV_FILENAME")
    local_validation_csv_path = os.getenv("VALIDATION_CSV_FILENAME")

    try:
        # Connecting to the Database
        conn = create_connection()

        test_folder_path = os.path.join(gcp_files_path, test_files_path)
        test_file_paths_dict = get_file_paths(bucket_name, creds_file_path, test_folder_path)

        validation_folder_path = os.path.join(gcp_files_path, validation_files_path)
        validation_file_paths_dict = get_file_paths(bucket_name, creds_file_path, validation_folder_path)

        # Download the CSV file from GCS
        download_csv_from_gcs(bucket_name, test_blob_name, local_test_csv_path, creds_file_path)
        download_csv_from_gcs(bucket_name, validation_blob_name, local_validation_csv_path, creds_file_path)

        # Load CSV data into DataFrame
        df_test = pd.read_csv(local_test_csv_path)
        df_validation = pd.read_csv(local_validation_csv_path)

        formatted_test_data, formatted_test_metadata = format_csv_data(df_test, test_file_paths_dict, dataset_type = "test")
        formatted_validation_data, formatted_validation_metadata = format_csv_data(df_validation, validation_file_paths_dict, dataset_type = "validation")

        insert_features_table_query = """
        INSERT INTO gaia_features(task_id, dataset_type, question, level, final_answer, file_name, file_path)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        insert_annotations_table_query = """
        INSERT INTO gaia_annotations(task_id, steps, number_of_steps, time_taken, tools, number_of_tools)
        VALUES (%s, %s, %s, %s, %s, %s)
        """

        cursor = conn.cursor()

        # for formatted_data in [formatted_test_data, formatted_validation_data]:
        #     for item in formatted_data:

        # Inserting test dataset into gaia_features 
        for item in formatted_test_data:
            cursor.execute(insert_features_table_query, (item['task_id'], item['dataset_type'], item['question'], item['level'], item['final_answer'], item['file_name'], item['file_path']))
        logger.info("SQL - load_parsed_data_to_db() - Insertion of test dataset into gaia_features done\n")

        # Inserting validation dataset into gaia_features 
        for item in formatted_validation_data:
            cursor.execute(insert_features_table_query, (item['task_id'], item['dataset_type'], item['question'], item['level'], item['final_answer'], item['file_name'], item['file_path']))
        logger.info("SQL - load_parsed_data_to_db() - Insertion of validation dataset into gaia_features done\n")

        # Inserting test dataset into gaia_annotations 
        for item in formatted_test_metadata:
            cursor.execute(insert_annotations_table_query, (item['task_id'], item['steps'], item['number_of_steps'], item['time_taken'], item['tools'], item['number_of_tools']))
        logger.info("SQL - load_parsed_data_to_db() - Insertion into gaia_annotations done\n")

        # Inserting test dataset into gaia_annotations 
        for item in formatted_validation_metadata:
            cursor.execute(insert_annotations_table_query, (item['task_id'], item['steps'], item['number_of_steps'], item['time_taken'], item['tools'], item['number_of_tools']))
        logger.info("SQL - load_parsed_data_to_db() - Insertion into gaia_annotations done\n")
        logger.info("SQL - load_parsed_data_to_db() - Insert statement executed successfully")
        conn.commit()

    except Exception as e:
        logger.error(f"SQL - load_parsed_data_to_db() - Error while loading parsed metadata into the Database table gaia_features and gaia_annotations = {e}")
        raise e
    finally:
        conn.close()


def cloud_uploader_pymupdf() -> None:
    '''Upload the contents extracted by PyMuPDF to Database and Google Cloud Storage Bucket'''

    pymupdf_logger.info("AIRFLOW - cloud_uploader_pymupdf() - Request to upload contents to database and cloud received")

    # Load env variables for Google Cloud
    credentials_file = os.path.join(os.getcwd(), os.getenv("GCS_CREDENTIALS_PATH"))
    bucket_name = os.getenv("BUCKET_NAME")
    bucket_storage_dir = os.getenv("BUCKET_STORAGE_DIR")
    
    base_dir = os.path.join(os.getcwd(), 'extracted_contents')
    dir_list = []
    
    try:
        dir_list = os.listdir(base_dir)
    
    except Exception as exception:
        dir_list = None
        pymupdf_logger.error(f"AIRFLOW - cloud_uploader_pymupdf() - Error fetching directory contents: {base_dir}")
        pymupdf_logger.error(exception)

    if dir_list is not None:
        
        conn = create_connection()
        if conn and conn.is_connected():
            try:
                
                # Creating Google Cloud Storage Client
                credentials = service_account.Credentials.from_service_account_file(credentials_file)
                client = storage.Client(credentials = credentials)
                bucket = client.bucket(bucket_name)

                # Open each directory in dir_list, and extract the contents of metadata, 
                # 'text' field in page_id.json, and upload them to the database
                for directory in dir_list:

                    ################## Load Metadata into the database and GCP Bucket ##################

                    # Parse the metadata file, and feed it to the database
                    metadata_file_path = os.path.join(base_dir, directory, 'metadata.json')

                    with open(metadata_file_path, 'r') as file:
                        metadata = json.load(file)

                    insert_metadata_sql = """
                    INSERT INTO pymupdf_info (file_name, title, format, creator, author, encryption, number_of_pages, number_of_words, number_of_images, number_of_tables) 
                    VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    cursor = conn.cursor()
                    pymupdf_logger.info(f"SQL - cloud_uploader_pymupdf() - Inserting metadata contents for file {directory}")

                    cursor.execute(insert_metadata_sql, (
                        str(directory),
                        metadata['title'] if metadata['title'] != '' else None,
                        metadata['format'],
                        metadata['creator'],
                        metadata['author'] if metadata['author'] != '' else None,
                        metadata['encryption'],
                        metadata['number_of_pages'],
                        metadata['number_of_words'],
                        metadata['number_of_images'],
                        metadata['number_of_tables']
                    ))

                    conn.commit()
                    pymupdf_logger.info(f"SQL - cloud_uploader_pymupdf() - Metadata inserted for file {directory}")

                    # Upload the metadata file to the bucket
                    pymupdf_logger.info(f"GCP - cloud_uploader_pymupdf() - Uploading metadata file to GCS Bucket for file {directory}")
                    
                    try:
                        metadata_blob = f"{bucket_storage_dir}/{directory}/" + os.path.basename(metadata_file_path)
                        blob= bucket.blob(metadata_blob)
                        blob.upload_from_filename(metadata_file_path)
                        pymupdf_logger.info(f"GCP - cloud_uploader_pymupdf() - Uploaded metadata file to GCS Bucket for file {directory}")
                    
                    except Exception as exception:
                        pymupdf_logger.error(f"GCP - cloud_uploader_pymupdf() - Error occured while uploading metadata file to GCS Bucket for file {directory}")
                        pymupdf_logger.error(exception)

                    ################## Load JSON, Image, and CSV into the GCP Bucket ##################

                    # Directories to upload
                    directories = ['CSV', 'JSON', 'Image']

                    try:
                        for folder in directories:
                            folder_dir = os.path.join(base_dir, directory, folder)
                            
                            if os.path.exists(folder_dir):
                                file_list = os.listdir(folder_dir)

                                if file_list:
                                    for file in file_list:
                                        pymupdf_logger.info(f"GCP - cloud_uploader_pymupdf() - Uploading {folder} directory and its contents to GCS Bucket for file {directory}")
                                        folder_blob = f"{bucket_storage_dir}/{directory}/{folder}/{file}"
                                        blob = bucket.blob(folder_blob)
                                        blob.upload_from_filename(os.path.join(folder_dir, file))
                                else:
                                    pymupdf_logger.info(f"GCP - cloud_uploader_pymupdf() - Uploading empty {folder} directory to GCS Bucket for file {directory}")
                                    folder_blob = f"{bucket_storage_dir}/{directory}/{folder}/"
                                    blob = bucket.blob(folder_blob)
                                    blob.upload_from_string('')
                                
                                pymupdf_logger.info(f"GCP - cloud_uploader_pymupdf() - Uploaded {folder} directory to GCS Bucket for file {directory}")
                            else:
                                pymupdf_logger.warning(f"GCP - cloud_uploader_pymupdf() - {folder} directory does not exist for file {directory}")

                    except Exception as exception:
                        pymupdf_logger.error(f"AIRFLOW - cloud_uploader_pymupdf() - Error occurred while uploading directories to GCP Bucket for {directory}")
                        pymupdf_logger.error(exception)


                    ################## Load page content into the database ##################

                    # Fetch the pdf_id 
                    pymupdf_logger.info(f"SQL - cloud_uploader_pymupdf() - Fetching pdf_id from database for file {directory}")
                        
                    cursor.execute(f"SELECT `pdf_id` FROM pymupdf_info WHERE file_name = '{str(directory)}'")
                    pdf_id = cursor.fetchone()[0]

                    pymupdf_logger.info(f"SQL - cloud_uploader_pymupdf() - Fetched pdf_id from database for file {directory}")
                    json_file_list = []

                    try:
                        json_dir = os.path.join(base_dir, directory, 'JSON')
                        json_file_list = os.listdir(json_dir)
                        
                    except Exception as exception:
                        json_file_list = None
                        pymupdf_logger.error(f"AIRFLOW - cloud_uploader_pymupdf() - Error fetching directory contents: {json_dir}")
                        pymupdf_logger.error(exception)
                        
                    if json_file_list is not None:
                        for jsonFile in json_file_list:
                            
                            page_file_path = os.path.join(json_dir, jsonFile)
                            pymupdf_logger.info(f"AIRFLOW - cloud_uploader_pymupdf() - Processing {directory}/JSON/{jsonFile}")
                            
                            with open(page_file_path, 'r') as _file:
                                page = json.load(_file)

                            # Read the 'text' content in each json file, and save them to the database
                            insert_text_query= """
                            INSERT INTO pymupdf_page_info (page_id, pdf_id, text)
                            VALUES (%s, %s, %s)
                            """

                            pymupdf_logger.info(f"SQL - cloud_uploader_pymupdf() - Inserting text for pdf_id {pdf_id}")
                            cursor.execute(insert_text_query, (
                                page['page_id'],
                                pdf_id, 
                                page['content']['text']
                            ))

                            conn.commit()
                            pymupdf_logger.info(f"SQL - cloud_uploader_pymupdf() - Inserted text for pdf_id {pdf_id}")

                            # Read the 'table', 'image' content in each json file, link them in the attachments and mappings table
                            items = ['table', 'image']

                            for item in items:
                                if len(page['content'][item]) > 0:
                                    for file_name in page['content'][item]:

                                        pymupdf_logger.info(f"SQL - cloud_uploader_pymupdf() - Inserting {item} record {file_name} for pdf_id {pdf_id}")
                                        insert_attachment_query = """
                                        INSERT INTO pymupdf_attachments (attachment_name, attachment_url)
                                        VALUES (%s, %s)
                                        """

                                        storage_path = f"{os.getenv('BUCKET_NAME')}/{os.getenv('BUCKET_STORAGE_DIR')}/{directory}/"
                                        storage_path += 'CSV' if item == 'table' else 'Image'
                                        cursor.execute(insert_attachment_query, (
                                            file_name, 
                                            storage_path
                                        ))

                                        conn.commit()
                                        pymupdf_logger.info(f"SQL - cloud_uploader_pymupdf() - Inserted {item} record {file_name} for pdf_id {pdf_id}")
                                        
                                        # Linking attachment, page, and pdf in the mappings table
                                        
                                        # Fetch the attachment_id for the recently inserted attachment
                                        pymupdf_logger.info(f"SQL - cloud_uploader_pymupdf() - Fetching attachment_id for {file_name} for file {directory}")
                                        cursor.execute(f"SELECT `attachment_id` FROM pymupdf_attachments ORDER BY `attachment_id` DESC LIMIT 1")
                                        attachment_id = cursor.fetchone()[0]
                                        pymupdf_logger.info(f"SQL - cloud_uploader_pymupdf() - Fetched attachment_id for {file_name} for file {directory}")
                                        
                                        # Insert the mapping into the mappings table
                                        pymupdf_logger.info(f"SQL - cloud_uploader_pymupdf() - Inserting attachment_mapping for {file_name} for page_id {page['page_id']} for pdf_id {pdf_id}")
                                        
                                        insert_mappings_query = """
                                        INSERT INTO pymupdf_attachment_mapping (pdf_id, page_id, attachment_id)
                                        VALUES (%s, %s, %s)
                                        """
                                        cursor.execute(insert_mappings_query, (
                                            pdf_id,
                                            page['page_id'],
                                            attachment_id
                                        ))
                                        
                                        conn.commit()
                                        pymupdf_logger.info(f"SQL - cloud_uploader_pymupdf() - Inserting attachment_mapping for {file_name} for page_id {page['page_id']} for pdf_id {pdf_id}")

            except Exception as exception:
                pymupdf_logger.error("AIRFLOW - cloud_uploader_pymupdf() - Error occured while inserting data into database")
                pymupdf_logger.error(exception)

            finally:
                if conn and conn.is_connected():
                    conn.close()
                    pymupdf_logger.info("DATABASE - cloud_uploader_pymupdf() - Connection to the database was closed")


def cloud_uploader_azure():
    logger.info("Azure - cloud_uploader_azure() - Inside cloud_uploader_azure() function")
    logger.info("Azure - cloud_uploader_azure() - Uploading file contents to the Database and files to GCS")

    # Load environmental variables
    creds_file_path = os.path.join(os.getcwd(), os.getenv("GCS_CREDENTIALS_PATH"))  
    bucket_name = os.getenv("BUCKET_NAME")
    azure_filepath = os.getenv("GCS_AZURE_FILEPATH")

    try:
        # Google Cloud Storage Client
        creds = service_account.Credentials.from_service_account_file(creds_file_path)
        client = storage.Client(credentials = creds)
        bkt = client.bucket(bucket_name)
        logger.info("Azure - cloud_uploader_azure() - GCS Client for Azure created successfully")

        # Connecting to the Database
        conn = create_connection()

        # Path to extracted pdf content
        dir_set = os.path.join(os.getcwd(), azure_filepath)
        dir_set_list = os.listdir(dir_set)

        for dir in dir_set_list:
            dir_pdf = os.path.join(dir_set, dir)
            dir_pdf_list = os.listdir(dir_pdf)

            # List of pdfs from the dataset_type
            for pdf in dir_pdf_list:          
                dir_folder = os.path.join(dir_pdf, pdf)
                dir_folder_list = os.listdir(dir_folder)
                try:
                    # Folders - ['Images', 'JSON', 'CSV']
                    for folder in dir_folder_list:
                        folder_dir = os.path.join(dir_folder, folder)
                        file_list = os.listdir(folder_dir)
                        gcs_file_path = os.path.join(azure_filepath, dir, pdf, folder) + '/'

                        if file_list:
                            # Files inside the Folders
                            for file in file_list:
                                logger.info(f"Azure - cloud_uploader_azure() - Uploading {file} file from {folder} folder and its contents to GCS")
                                file_blob = os.path.join(gcs_file_path, file)
                                blob = bkt.blob(file_blob)
                                blob.upload_from_filename(file_blob)
                        else:
                            logger.info(f"Azure - cloud_uploader_azure() - Uploading empty {folder} folder to GCS")
                            blob = bkt.blob(gcs_file_path)
                            blob.upload_from_string('')
                except Exception as e:
                    logger.error(f"Azure - cloud_uploader_azure() - Error occured while uploading files to GCS")
                    raise e
                
                try:
                    # dir_folder = curr_dir + azure_doc_extract/test/pdf_filename + "JSON"
                    json_dir = os.path.join(dir_folder, 'JSON')
                    json_file_list = os.listdir(json_dir)
                    logger.info(f"PDF Filename = {pdf}")

                    for jsonFile in json_file_list:
                        # cwd + /azure_doc_extract/test/be353748-74eb-4904-8f17-f180ce087f1a/JSON/page_1.json
                        page_file_path = os.path.join(json_dir, jsonFile)
                        logger.info(f"Azure - cloud_uploader_azure() - Processing {azure_filepath}/{dir}/{pdf}/JSON/{jsonFile}")
                        with open(page_file_path, 'r') as _file:
                            page = json.load(_file)
                        
                        logger.info(f"Page id = {page['page_number']}")
                        # Read the 'text' content in each json file, and save them to the database
                        insert_text_query = """
                        INSERT INTO azure_info (page_id, text, pdf_filename)
                        VALUES (%s, %s, %s)
                        """

                        logger.info(f"SQL - cloud_uploader_azure() - Inserting text for pdf {pdf}")
                        cursor = conn.cursor()
                        cursor.execute(insert_text_query, (page['page_number'], page['text'], pdf))

                        conn.commit()
                        logger.info(f"SQL - cloud_uploader_azure() - Inserted text for pdf {pdf}")


                except Exception as e:
                    logger.error(f"Azure - cloud_uploader_azure() - Error fetching directory contents: {json_dir}")
                    raise e        

    except Exception as e:
        logger.error("Azure - cloud_uploader_azure() - Error while executing cloud_uploader_azure() function")
        raise e

    finally:
        conn.close()
        logger.info("Azure - cloud_uploader_azure() - Connection to the database closed")
        logger.info("Azure - cloud_uploader_azure() - Uploading content to database and GCS successful")


def download_file_from_gcs(bucket_name, gcp_files_path, file_name, creds_file_path, download_folder):
    logger.info("Adobe - pdfDownloader_driver_func() -  download_file_from_gcs() - Downloading each pdf files from the folder")
    creds = service_account.Credentials.from_service_account_file(creds_file_path)
    client = storage.Client(credentials=creds)
    bkt = client.bucket(bucket_name)

    logger.info(f"Adobe - pdfDownloader_driver_func() -  download_file_from_gcs() - Downloading: {file_name}")
    blob = bkt.blob(file_name)

    # Create the download directory if it doesn't exist
    if not os.path.exists(download_folder):
        os.makedirs(download_folder)  # Create the folder

    # Download the file to the download folder
    destination_file_path = os.path.join(download_folder, os.path.basename(file_name))  # Full path for the downloaded file
    blob.download_to_filename(destination_file_path)  # Download the blob to the destination path
    logger.info(f"Adobe - pdfDownloader_driver_func() -  download_file_from_gcs() - Downloaded to: {destination_file_path}")

def download_pdf_files_for_adobe(bucket_name, gcp_files_path, folder_name, creds_file_path, download_folder):
    logger.info("Adobe - pdfDownloader_driver_func() -  download_pdf_files()")
    logger.info(f"Adobe - pdfDownloader_driver_func() -  download_pdf_files() - Downloading files from {bucket_name}/{gcp_files_path}/{folder_name}")
    creds = service_account.Credentials.from_service_account_file(creds_file_path)
    client = storage.Client(credentials=creds)
    bkt = client.bucket(bucket_name)

    # Construct the correct prefix without duplication
    prefix = os.path.join(gcp_files_path, folder_name)  # This should create 'files/test' or 'files/validation'
    logger.info(f"Adobe - pdfDownloader_driver_func() -  download_pdf_files() - Using prefix: {prefix}")  # Log the prefix being used

    # List all blobs with the specified prefix
    blobs = bkt.list_blobs(prefix=prefix)

    # Log all blobs found for debugging
    logger.info("Adobe - pdfDownloader_driver_func() -  download_pdf_files() - Listing all blobs found:")
    for blob in blobs:
        logger.info(f"Adobe - pdfDownloader_driver_func() -  download_pdf_files() - Blob found: {blob.name}")  # Log the names of all blobs found

    # Reset the blobs generator
    blobs = bkt.list_blobs(prefix=prefix)  # Re-initialize to list blobs again

    file_count = 0  # Track number of files found

    for blob in blobs:
        if blob.name.lower().endswith('.pdf'):  # Ensure case insensitivity
            file_count += 1
            logger.info(f"Adobe - pdfDownloader_driver_func() -  download_pdf_files() - Found PDF: {blob.name}")
            download_file_from_gcs(bucket_name, gcp_files_path, blob.name, creds_file_path, download_folder)

    logger.info(f"Adobe - pdfDownloader_driver_func() -  download_pdf_files() - Total PDFs found: {file_count}")
    if file_count == 0:
        logger.warning("Adobe - pdfDownloader_driver_func() -  download_pdf_files() - No PDF files found in the specified directory.")

def pdfDownloader_driver_func():
    load_dotenv()
    logger.info("Adobe - pdfDownloader_driver_func() - Inside pdfDownloader_driver_func() function")
    logger.info("Adobe - pdfDownloader_driver_func() - Downloading PDF files from GCS to local")

    # Environment variables
    bucket_name = os.getenv("BUCKET_NAME")
    gcp_files_path = os.getenv("GCP_FILES_PATH")
    test_files_path = os.getenv("TEST_FILE_PATH")
    validation_files_path = os.getenv("VALIDATION_FILE_PATH")
    creds_file_path = os.getenv("GCS_CREDENTIALS_PATH")

    # Create a folder for downloaded PDFs
    download_folder = os.path.join(os.getcwd(), 'downloaded_pdfs')

    download_pdf_files_for_adobe(bucket_name, gcp_files_path, test_files_path, creds_file_path, download_folder)
    download_pdf_files_for_adobe(bucket_name, gcp_files_path, validation_files_path, creds_file_path, download_folder)
    logger.info("Adobe - pdfDownloader_driver_func() - All PDF files successfully downloaded from GCS to local")

class ExtractTextInfoFromPDF:
    def __init__(self, pdf_file):
        logger.info("Adobe - adobeExtractor_driver_func() - ExtractTextInfoFromPDF - process_all_pdfs_in_directory")
        self.pdf_file = pdf_file
        self.zip_file = self.create_zip_file_path()

        try:
            with open(self.pdf_file, 'rb') as file:
                input_stream = file.read()

            with open('Adobe_Credentials.json', 'r') as file:
                credentials = json.load(file)

            # Initial setup, create credentials instance
            credentials = ServicePrincipalCredentials(
                client_id=credentials['CLIENT_ID'],
                client_secret=credentials['CLIENT_SECRETS'][0]
            )

            # Creates a PDF Services instance
            pdf_services = PDFServices(credentials=credentials)

            # Creates an asset from the source file and upload
            input_asset = pdf_services.upload(input_stream=input_stream, mime_type=PDFServicesMediaType.PDF)

            # Create parameters for the job
            extract_pdf_params = ExtractPDFParams(
                elements_to_extract=[ExtractElementType.TEXT, ExtractElementType.TABLES],
                elements_to_extract_renditions=[ExtractRenditionsElementType.FIGURES],
                table_structure_type=TableStructureType.CSV
            )

            # Creates a new job instance
            extract_pdf_job = ExtractPDFJob(input_asset=input_asset, extract_pdf_params=extract_pdf_params)

            # Submit the job and get the job result
            location = pdf_services.submit(extract_pdf_job)
            pdf_services_response = pdf_services.get_job_result(location, ExtractPDFResult)

            # Get content from the resulting asset
            result_asset: CloudAsset = pdf_services_response.get_result().get_resource()
            stream_asset: StreamAsset = pdf_services.get_content(result_asset)

            # Creates an output stream and copy stream asset's content to it
            with open(self.zip_file, "wb") as file:
                file.write(stream_asset.get_input_stream())

            self.process_json_from_zip()

        except (ServiceApiException, ServiceUsageException, SdkException) as e:
            logger.warning(f"Adobe - adobeExtractor_driver_func() - ExtractTextInfoFromPDF - Exception encountered while executing operation on {self.pdf_file}: {e}")

    def create_zip_file_path(self) -> str:
        logger.info("Adobe - adobeExtractor_driver_func() - ExtractTextInfoFromPDF - create_zip_file_path")
        now = datetime.now()
        time_stamp = now.strftime("%Y-%m-%dT%H-%M-%S")
        output_folder = os.path.join(os.getcwd(), 'output_folder')
        # output_folder = "/Users/gomathyselvamuthiah/Desktop/ass2/airflow/output_folder"
        os.makedirs(output_folder, exist_ok=True)
        return f"{output_folder}/extract_{os.path.basename(self.pdf_file).replace('.pdf', '')}_{time_stamp}.zip"

    def process_json_from_zip(self):
        logger.info("Adobe - adobeExtractor_driver_func() - ExtractTextInfoFromPDF - process_json_from_zip")
        # Read and extract data from the JSON file in the ZIP
        with zipfile.ZipFile(self.zip_file, 'r') as archive:
            jsonentry = archive.open('structuredData.json')
            jsondata = jsonentry.read()
            data = json.loads(jsondata)

            for element in data.get("elements", []):
                if element["Path"].endswith("/H1"):
                    print(f"Text from {self.pdf_file}: {element['Text']}")

def process_all_pdfs_in_directory(directory_path):
    logger.info("Adobe - adobeExtractor_driver_func() - process_all_pdfs_in_directory() - Processing all the pdfs present in the directory")
    for filename in os.listdir(directory_path):
        if filename.endswith(".pdf"):
            pdf_file_path = os.path.join(directory_path, filename)
            logger.info(f"Adobe - adobeExtractor_driver_func() - process_all_pdfs_in_directory() - Processing file: {pdf_file_path}")
            ExtractTextInfoFromPDF(pdf_file_path)
    logger.info("Adobe - adobeExtractor_driver_func() - process_all_pdfs_in_directory() - All pdf contents are processed successfully")

def adobeExtractor_driver_func():
    logger.info("Adobe - adobeExtractor_driver_func() - Inside adobeExtractor_driver_func() function")
    logger.info("Adobe - adobeExtractor_driver_func() - Extracting file contents from pdf files using Adobe")
    downloaded_pdfs_folder = os.path.join(os.getcwd(), 'downloaded_pdfs')
    # downloaded_pdfs_folder = '/Users/gomathyselvamuthiah/Desktop/ass2/airflow/downloaded_pdfs'
    process_all_pdfs_in_directory(downloaded_pdfs_folder)
    logger.info("Adobe - adobeExtractor_driver_func() - Extracting file contents from pdf files using Adobe executed successfully")




def cloud_uploader_adobe():
    logger.info("Adobe - clouduploader_adobe() - Inside clouduploader_adobe() function")
    logger.info("Adobe - clouduploader_adobe() - Uploading file contents to the Database and files to GCS")

    # Load environmental variables
    bucket_name = os.getenv('BUCKET_NAME')
    extracted_filepath = os.getenv('EXTRACTED_FILEPATH')
    unzip_filepath = os.getenv("UNZIP_FILEPATH")
    gcs_adobe_filepath = os.getenv("GCS_ADOBE_FILEPATH")
    creds_file_path = os.getenv("GCS_CREDENTIALS_PATH")

    conn = None
    try:
        # Google Cloud Storage Client
        creds = service_account.Credentials.from_service_account_file(creds_file_path)
        client = storage.Client(credentials=creds)
        bucket = client.bucket(bucket_name)
        logger.info("Adobe - clouduploader_adobe() - GCS Client for Adobe created successfully")

        # Connecting to the Database
        conn = create_connection()
        if conn is None:
            logger.error("Adobe - clouduploader_adobe() - Could not establish database connection")
            return

        # Path to extracted pdf content
        zip_file_dir = os.path.join(os.getcwd(), extracted_filepath)
        zip_file_list = os.listdir(zip_file_dir)
        for zip_file in zip_file_list:
            if zip_file.endswith('.zip'):
                zip_file_path = os.path.join(zip_file_dir, zip_file)

                pdf_base_name = os.path.splitext(os.path.basename(zip_file_path))[0]
                logger.info(f"Adobe - clouduploader_adobe() - Processing PDF file: {pdf_base_name}")

                # Create directories for CSV, JSON, and IMAGES
                local_csv_dir = os.path.join(os.getcwd(), unzip_filepath, pdf_base_name, 'CSV')
                local_json_dir = os.path.join(os.getcwd(), unzip_filepath, pdf_base_name, 'JSON')
                local_images_dir = os.path.join(os.getcwd(), unzip_filepath, pdf_base_name, 'IMAGES')
                os.makedirs(local_csv_dir, exist_ok=True)
                os.makedirs(local_json_dir, exist_ok=True)
                os.makedirs(local_images_dir, exist_ok=True)

                logger.info(f'Adobe - clouduploader_adobe() - Unzipping {zip_file_path} for {pdf_base_name}')
                with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
                    for file in zip_ref.namelist():
                        # Extract based on file type
                        if file.endswith('.csv'):
                            zip_ref.extract(file, local_csv_dir)
                            logger.info(f'Adobe - clouduploader_adobe() - Extracted {file} to {local_csv_dir}')
                        elif file.endswith('.json'):
                            zip_ref.extract(file, local_json_dir)
                            logger.info(f'Adobe - clouduploader_adobe() - Extracted {file} to {local_json_dir}')
                        elif file.endswith(('.png', '.jpg', '.jpeg', '.gif')):
                            zip_ref.extract(file, local_images_dir)
                            logger.info(f'Adobe - clouduploader_adobe() - Extracted {file} to {local_images_dir}')

                # Create empty folders in GCS for CSV, JSON, and IMAGES
                for folder in ['CSV', 'JSON', 'IMAGES']:
                    empty_folder = os.path.join(gcs_adobe_filepath, pdf_base_name, folder)
                    blob = bucket.blob(empty_folder + '/')
                    blob.upload_from_string('')
                    logger.info(f'Adobe - clouduploader_adobe() - Created empty GCS folder: gs://{bucket_name}/{empty_folder}')

                # Upload extracted files to GCS
                for local_dir, gcs_dir in [(local_csv_dir, 'CSV'), (local_json_dir, 'JSON'), (local_images_dir, 'IMAGES')]:
                    for root, _, files in os.walk(local_dir):
                        for file in files:
                            local_file_path = os.path.join(root, file)
                            gcs_file_path = os.path.join(gcs_adobe_filepath, pdf_base_name, gcs_dir, f"{pdf_base_name}_{file}")
                            logger.info(f'Adobe - clouduploader_adobe() - Uploading {local_file_path} to gs://{bucket_name}/{gcs_file_path}')
                            blob = bucket.blob(gcs_file_path)
                            blob.upload_from_filename(local_file_path)
                            logger.info(f'Adobe - clouduploader_adobe() - Uploaded {local_file_path} to gs://{bucket_name}/{gcs_file_path}')

                # Process structuredData.json and insert into DB
                json_dir = os.path.join(unzip_filepath, pdf_base_name, 'JSON')
                structured_data_path = os.path.join(json_dir, 'structuredData.json')
                if os.path.exists(structured_data_path):
                    logger.info(f"Adobe - clouduploader_adobe() - Processing {structured_data_path}")

                    with open(structured_data_path, 'r') as json_file:
                        data = json.load(json_file)

                    # # is_encrypted = data['extended_metadata'].get('is_encrypted', False)
                    # is_encrypted_str = data['extended_metadata'].get('is_encrypted', '0')  # Default to '0' if not found
                    is_encrypted = data['extended_metadata'].get('is_encrypted')
                    page_count = data['extended_metadata'].get('page_count', 0)
                    elements = data.get('elements', [])

                    page_content = {}
                    for element in elements:
                        if 'Text' in element:
                            page_id = int(element.get('Page', -1))
                            if page_id != -1:
                                page_content.setdefault(page_id, "")
                                page_content[page_id] += element['Text']

                    # Insert each page's content into DB
                    insert_text_query = """
                    INSERT INTO adobe_info (page_id, text, number_of_pages, is_encrypted, pdf_filename)
                    VALUES (%s, %s, %s, %s, %s)
                    """
                    cursor = conn.cursor()

                    for page_id, content in page_content.items():
                        logger.info(f"SQL - clouduploader_adobe() - Inserting content for page {page_id}")
                        cursor.execute(insert_text_query, (page_id, content, page_count, is_encrypted, pdf_base_name))
                        conn.commit()

                    logger.info(f"SQL - clouduploader_adobe() - Inserted all data for {pdf_base_name}")

    except Exception as e:
        logger.error(f"Adobe - clouduploader_adobe() - Error: {e}")
        raise e

    finally:
        if conn:
            conn.close()
            logger.info("Adobe - clouduploader_adobe() - Database connection closed")
        logger.info("Adobe - clouduploader_adobe() - Upload completed successfully")


# DAG configuration
default_args = {
    'owner'     : 'airflow',
    'start_date': days_ago(1),
    'retries'   : 1,
}

# Create DAG with tasks
with DAG(
    dag_id = 'pdf_content_extraction', 
    default_args = default_args, 
    schedule_interval = '@once'
) as dag:
    
    download_pdf_task = PythonOperator(
        task_id='pdf_downloader',
        python_callable=pdf_downloader
    )

    fileLoader_task = PythonOperator(
        task_id = 'fileLoader',
        python_callable = fileLoader_driver_func
    )

    fileParser_task = PythonOperator(
        task_id = 'fileParser',
        python_callable = fileParser_driver_func
    )

    azure_pdfFileExtractor = PythonOperator(
        task_id = 'azure_pdfFileExtractor',
        python_callable = azure_pdfFileExtractor_driver_func
    )
    
    extract_pymupdf_task = PythonOperator(
        task_id='extract_content_pymupdf',
        python_callable=extract_content_pymupdf
    )

    extract_metadata_task = PythonOperator(
        task_id='extract_metadata',
        python_callable=extract_metadata
    )

    setup_tables_task = PythonOperator(
        task_id='setup_tables',
        python_callable=setup_tables
    )

    load_database_task = PythonOperator(
        task_id = 'loadDatabase',
        python_callable = loadDatabase_driver_func
    )
    
    cloud_uploader_pymupdf_task = PythonOperator(
        task_id='cloud_uploader_pymupdf',
        python_callable=cloud_uploader_pymupdf
    )

    cloud_uploader_azure_task = PythonOperator(
        task_id='cloud_uploader_azure',
        python_callable=cloud_uploader_azure
    )

    pdf_downloader_adobe_task = PythonOperator(
        task_id='pdfDownloader_driver_func',
        python_callable=pdfDownloader_driver_func
    )

    adode_extractor_task = PythonOperator(
        task_id='adobeExtractor_driver_func',
        python_callable=adobeExtractor_driver_func
    )

    cloud_uploader_adobe_task = PythonOperator(
        task_id='cloud_uploader_adobe',
        python_callable=cloud_uploader_adobe
    )


    
    # Task Dependencies
    download_pdf_task >> pdf_downloader_adobe_task >> fileLoader_task >> fileParser_task >> azure_pdfFileExtractor >> adode_extractor_task >> extract_pymupdf_task >> extract_metadata_task >> setup_tables_task >> load_database_task >> cloud_uploader_pymupdf_task >> cloud_uploader_azure_task >> cloud_uploader_adobe_task
