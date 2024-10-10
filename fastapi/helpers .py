import os
import csv
import jwt
import docx
import json
import hmac
import PyPDF2
import hashlib
import logging
import openpyxl
import tiktoken
import datetime
from dotenv import load_dotenv
from typing import Literal, Any
from google.cloud import storage
from passlib.context import CryptContext
from datetime import timezone, timedelta
from google.oauth2 import service_account
from fastapi import status, HTTPException

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

# Secret key used for password hashing and JWT token encoding
SECRET_KEY = "Being shown a random sentence and using it to complete a paragraph each day can be an excellent way to begin any writing session"


# Set context for password hashing
password_context = CryptContext(
    schemes                         = ["sha256_crypt"],
    sha256_crypt__default_rounds    = int(os.getenv('SHA256_ROUNDS')),
    deprecated                      = "auto"
)

# Helper function to hash passwords
def get_password_hash(password: str) -> str:
    '''Helper function to return hashed passwords'''

    logger.info("INTERNAL - Request to hash password received")
    hash_hex = None

    try:
        # Convert the secret key to bytes for hashing
        secret_key = SECRET_KEY.encode()
        
        # Create an HMAC hash object using the secret key and the password
        hash_object = hmac.new(secret_key, msg=password.encode(), digestmod=hashlib.sha256)
        
        # Generate the hex digest of the hash
        hash_hex = hash_object.hexdigest()

    except Exception as exception:
        logger.error("Error: get_password_hash() encountered an error")
        logger.error(exception)
        
    return hash_hex


# Helper function to create a JWT token with an expiration time
def create_jwt_token(data: dict) -> dict[str, Any]:
    '''Helper function to create a JWT token with an expiration time'''
    
    # Set token expiration time to 'x' minutes from the current time
    expiration = datetime.datetime.now(timezone.utc) + timedelta(minutes=60)
    
    # Create the token payload with expiration and provided data
    token_payload = {
        "expiration": str(expiration), 
        **data
    }
    
    # Encode the payload using the secret key and HS256 algorithm to create the token
    token = jwt.encode(token_payload, SECRET_KEY, algorithm="HS256")
    
    token_dict = {
        'token'         : token,
        'token_type'    : "bearer"
    }

    return token_dict


# Function to decode the JWT token and verify its validity
def decode_jwt_token(token: dict):
    '''Function to decode the JWT token and verify its validity'''

    try:
        # Decode the JWT token
        decoded_token = jwt.decode(
            token['token'], 
            SECRET_KEY, 
            algorithms = ["HS256"]
        )
        
        return decoded_token
    
    except Exception as exception:
        logger.error("Error: decode_jwt_token() encountered an error")
        logger.error(exception)
        
        raise HTTPException(
            status_code = status.HTTP_401_UNAUTHORIZED,
            detail      = "Token expired",
            headers     = {"WWW-Authenticate": "Bearer"},
        )
    
# Helper function to check if JWT token is expired
def validate_token(token: dict):
    '''Helper function to check if JWT token is expired'''

    is_expired = True
    try:

        payload = jwt.decode(
            token['token'], 
            SECRET_KEY, 
            algorithms = ["HS256"]
        )

        expiration = datetime.fromisoformat(payload['expiration'])
        
        # Check if token has expired
        current_time = datetime.datetime.now(timezone.utc)
        if current_time < expiration:
            is_expired = False
        
    except Exception as exception:
        logger.error("Error: validate_token() encountered an error")
        logger.error(exception)
    
    finally:
        return is_expired


# Helper function to verify passwords
def verify_password(plain_password: str, hashed_password: str) -> bool:
    '''Helper function to verify passwords'''

    rehashed_pass= get_password_hash(plain_password)

    return rehashed_pass == hashed_password


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
        logger.error("Error: download_files_from_gcs() encountered an error")
        logger.error(exception)
    
    return download_status