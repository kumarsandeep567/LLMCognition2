from dotenv import load_dotenv
from google.cloud import storage
from google.oauth2 import service_account
from azure.ai.formrecognizer import DocumentAnalysisClient
from azure.core.credentials import AzureKeyCredential
import logging
import os
import json
import pandas as pd
import io
import base64

# Logger configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def download_pdf_from_gcs(bucket_name, file_name, creds_file_path):
    creds = service_account.Credentials.from_service_account_file(creds_file_path)
    client = storage.Client(credentials=creds)
    blob = client.bucket(bucket_name).blob(file_name)

    logger.info(f"Downloading: {file_name}")
    pdf_bytes = blob.download_as_bytes()  # Downloading as bytes
    return pdf_bytes

def download_pdf_files(bucket_name, gcp_files_path, folder_name, creds_file_path):
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

    return pdf_data_list, pdf_file_names

def extract_data_from_pdf(pdf_data, endpoint, key):
    logger.info("Initializing Document Analysis Client")
    client = DocumentAnalysisClient(endpoint=endpoint, credential=AzureKeyCredential(key))

    logger.info("Sending PDF for analysis")
    poller = client.begin_analyze_document("prebuilt-document", document=io.BytesIO(pdf_data))
    result = poller.result()

    # Initialize containers for extracted data
    extracted_data = {
        "tables": [],
        "text": [],
        "images": []
    }

    logger.info("Extracting data from PDF")
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

    return extracted_data

def save_data(gcs_extract_file_path, folder_name, extracted_data, pdf_file_name):
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
        logger.info(f"Saved table {i} to {csv_file_path}.")

    # Save text to JSON
    for page_number, text in enumerate(extracted_data['text'], 1):
        json_file_path = os.path.join(json_pdf_folder, f'page_{page_number}.json')
        with open(json_file_path, 'w') as json_file:
            json.dump({"page_number": page_number, "text": text}, json_file)
        logger.info(f"Saved text for page {page_number} to {json_file_path}.")

    # Save images
    for i, image in enumerate(extracted_data['images']):
        image_file_path = os.path.join(image_pdf_folder, f'image_page_{image["page_number"]}_{i}.png')
        if image['image_content'].startswith('data:image/png;base64,'):
            _, encoded = image['image_content'].split(',', 1)
            image_data = base64.b64decode(encoded)
            with open(image_file_path, 'wb') as img_file:
                img_file.write(image_data)
            logger.info(f"Saved image from page {image['page_number']} to {image_file_path}.")
        else:
            logger.warning(f"Unsupported image format on page {image['page_number']}.")

def main():
    logger.info("Inside main function")

    # Load Environment variables
    load_dotenv()

    bucket_name = os.getenv("BUCKET_NAME")
    gcp_files_path = os.getenv("GCP_FILES_PATH")
    test_files_path = os.getenv("TEST_FILE_PATH")
    validation_files_path = os.getenv("VALIDATION_FILE_PATH")
    creds_file_path = os.getenv("GCS_CREDENTIALS_PATH")
    azure_endpoint = os.getenv("AZURE_ENDPOINT")
    azure_key = os.getenv("AZURE_KEY")
    gcs_extract_file_path = os.getenv("GCS_AZURE_FILEPATH")

    # Download PDFs
    for folder_name in [test_files_path, validation_files_path]:
        pdf_data_list, pdf_file_names = download_pdf_files(bucket_name, gcp_files_path, folder_name, creds_file_path)

        # Extract data from downloaded PDFs
        for pdf_data, pdf_file_name in zip(pdf_data_list, pdf_file_names):
            logger.info(f"Extracting data from {pdf_file_name}")
            extracted_data = extract_data_from_pdf(pdf_data, azure_endpoint, azure_key)
            save_data(gcs_extract_file_path, folder_name, extracted_data, pdf_file_name)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.error(f"Error while executing main function: {e}")