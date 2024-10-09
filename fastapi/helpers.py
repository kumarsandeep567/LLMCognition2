import os
import csv
import json
import docx
import PyPDF2
import logging
import openpyxl
import tiktoken
import datetime
from typing import Literal
from dotenv import load_dotenv
from google.cloud import storage
from google.oauth2 import service_account
from passlib.context import CryptContext

# Load env variables
load_dotenv()

# ============================= Logger : Begin =============================

# Initialize logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

# Log to console (dev only)
if os.getenv('APP_ENV') == "development":
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    logger.addHandler(handler)

# Also log to a file
file_handler = logging.FileHandler(os.getenv('FASTAPI_LOG_FILE', "fastapi_errors.log"))
file_handler.setFormatter(formatter)
logger.addHandler(file_handler) 

# ============================= Logger : End ===============================

# Set context for password hashing
password_context = CryptContext(
    schemes                         = ["sha256_crypt"],
    sha256_crypt__default_rounds    = int(os.getenv('SHA256_ROUNDS')),
    deprecated                      = "auto"
)

# Helper function to hash passwords
def get_password_hash(password: str) -> str:
    '''Helper function to return hashed passwords'''

    return password_context.hash(password)


# Helper function to verify passwords
def verify_password(plain_password: str, hashed_password: str) -> bool:
    '''Helper function to verify passwords'''

    return password_context.verify(plain_password, hashed_password)


# Helper function to count tokens
def count_tokens(text: str) -> int:
    '''Helper function to count tokens for the GPT-4o model'''

    encoding = tiktoken.encoding_for_model("gpt-4o")
    return len(encoding.encode(text))


# Helper function to provide rectification strings
def rectification_helper() -> Literal:
    '''Helper function to provide rectification strings'''

    return "The answer you provided is incorrect. I have attached the question and the steps to find the correct answer for the question. Please perform them and report the correct answer."


# Helper function to generate response restriction
def generate_restriction(final_answer: str) -> str:
    '''Helper function to generate response restriction'''

    words = final_answer.split()
    if len(words) <= 10:
        return f"Restrict your response to {len(words)} words only. No yapping."
    elif final_answer.replace(" ", "").isdigit():
        return "Provide only numerical values in your response. No yapping."
    else:
        return "No yapping."
    
# Helper function to check if object is json serializable
def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""
    
    if isinstance(obj, datetime.datetime):
        try:
            return obj.isoformat()
        except:
            return str(obj)
    return str(obj)


# Helper function to extract contents from a file
def extract_file_content(file_path: str) -> str:
    """Extract content from various file types."""
    
    _, file_extension = os.path.splitext(file_path)
    file_extension = file_extension.lower()

    logger.info("INTERNAL - Request to extract file contents received")

    try:

        if file_extension in ['.txt', '.py', '.pdb']:
            logger.info("INTERNAL - Processing .txt or similar file")
            with open(file_path, 'r') as file:
                return file.read()

        elif file_extension == '.pdf':
            logger.info("INTERNAL - Processing .pdf file")
            with open(file_path, 'rb') as file:
                reader = PyPDF2.PdfReader(file)
                return ' '.join([page.extract_text() for page in reader.pages])

        elif file_extension == '.docx':
            logger.info("INTERNAL - Processing .docx file")
            doc = docx.Document(file_path)
            return ' '.join([paragraph.text for paragraph in doc.paragraphs])

        elif file_extension in ['.xlsx', '.csv']:
            if file_extension == '.xlsx':
                logger.info("INTERNAL - Processing .xlsx file")
                workbook = openpyxl.load_workbook(file_path)
                sheet = workbook.active
                data = [[json_serial(cell.value) for cell in row] for row in sheet.iter_rows()]
            
            # CSV files
            else: 
                logger.info("INTERNAL - Processing .csv file")
                with open(file_path, 'r') as file:
                    reader = csv.reader(file)
                    data = [[json_serial(value) for value in row] for row in reader]
            return json.dumps(data)

        elif file_extension == '.jsonld':
            logger.info("INTERNAL - Processing .json file")
            with open(file_path, 'r') as file:
                return json.load(file)

        else:
            return None

    except Exception as e:
        logger.error(f"Error extracting content from {file_path}: {str(e)}")
        return None
    
    logger.info("INTERNAL - File content extraction completed")


# Helper function to download files from GCP bucket
def download_files_from_gcs() -> bool:
    '''Helper function to download files from GCP bucket'''

    logger.info("INTERNAL - GCP file download request received")

    # Environment variables
    bucket_name = os.getenv("BUCKET_NAME")
    gcp_folder_path = os.getenv("GCP_FILES_PATH")
    creds_file_path = os.path.join(os.getcwd(), os.getenv("GCS_CREDENTIALS_FILE"))

    download_status = False
    
    try:

        # Authenticate using the service account file
        creds = service_account.Credentials.from_service_account_file(creds_file_path)
        client = storage.Client(credentials = creds)
        bkt = client.bucket(bucket_name)

        # List all blobs in the folder
        blobs = bkt.list_blobs(prefix = gcp_folder_path)

        # Iterate through the blobs and download each file
        for blob in blobs:

            # Extract the file name from the blob's full path
            file_name = os.path.basename(blob.name)
            
            # Skip if it's just the folder itself and not a file
            if not file_name: 
                continue

            # Create the download directory if missing
            if not os.path.exists(os.getenv('DOWNLOAD_DIR')):
                os.makedirs(os.getenv('DOWNLOAD_DIR'))
            
            # File to download and path to save
            local_file_path = os.path.join(os.getenv('DOWNLOAD_DIR'), file_name)

            # Download the file
            blob.download_to_filename(local_file_path)
        
        download_status = True
        logger.info("INTERNAL - GCP file download completed")
    
    except Exception as exception:
        logger.error("Error: download_files_from_gcs() encountered a an error")
        logger.error(exception)
    
    return download_status