from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

import os
import json
import time
import shutil
import logging
import pymupdf
import pandas as pd
import mysql.connector
from dotenv import load_dotenv
from unidecode import unidecode
from google.cloud import storage
from mysql.connector import Error
from google.oauth2 import service_account
from huggingface_hub import hf_hub_download, list_repo_files


# Load the environment variables
load_dotenv()

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
            "drop_adobe_attachment_mapping_table"   : "DROP TABLE IF EXISTS adobe_attachment_mapping;",
            "drop_adobe_page_info_table"            : "DROP TABLE IF EXISTS adobe_page_info;",
            "drop_adobe_attachment_table"           : "DROP TABLE IF EXISTS adobe_attachments;",
            "drop_adobe_info_table"                 : "DROP TABLE IF EXISTS adobe_info;",
            "drop_azure_attachment_mapping_table"   : "DROP TABLE IF EXISTS azure_attachment_mapping;",
            "drop_azure_page_info_table"            : "DROP TABLE IF EXISTS azure_page_info;",
            "drop_azure_attachment_table"           : "DROP TABLE IF EXISTS azure_attachments;",
            "drop_azure_info_table"                 : "DROP TABLE IF EXISTS azure_info;",
        },
        "create_tables": {
            "create_features_table": """
                CREATE TABLE gaia_features(
                    task_id VARCHAR(255) PRIMARY KEY,
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
                    password VARCHAR(255) NOT NULL
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
                CREATE TABLE adobe_info(
                    pdf_id INT PRIMARY KEY AUTO_INCREMENT,
                    file_name VARCHAR(255) NOT NULL,
                    title VARCHAR(255) DEFAULT NULL,
                    format varchar(255),
                    creator VARCHAR(255) DEFAULT NULL,
                    author VARCHAR(255) DEFAULT NULL,
                    encryption VARCHAR(20) DEFAULT NULL,
                    number_of_pages INT,
                    number_of_words INT,
                    number_of_images INT,
                    number_of_tables INT
                );
            """,
            "create_adobe_page_info_table": """
                CREATE TABLE adobe_page_info(
                    info_id INT PRIMARY KEY AUTO_INCREMENT,
                    page_id INT NOT NULL,
                    text TEXT DEFAULT NULL,
                    pdf_id INT NOT NULL,
                    FOREIGN KEY (pdf_id) REFERENCES adobe_info(pdf_id),
                    INDEX (page_id)
                );
            """,
            "create_adobe_attachment_table": """
                CREATE TABLE adobe_attachments(
                    attachment_id INT PRIMARY KEY AUTO_INCREMENT,
                    attachment_name VARCHAR(255) NOT NULL,
                    attachment_url TEXT NOT NULL
                );
            """,
            "create_adobe_attachment_mapping_table": """
                CREATE TABLE adobe_attachment_mapping(
                    mapping_id INT PRIMARY KEY AUTO_INCREMENT,
                    pdf_id INT NOT NULL,
                    page_id INT NOT NULL,
                    attachment_id INT NOT NULL,
                    FOREIGN KEY (pdf_id) REFERENCES adobe_info(pdf_id),
                    FOREIGN KEY (page_id) REFERENCES adobe_page_info(page_id),
                    FOREIGN KEY (attachment_id) REFERENCES adobe_attachments(attachment_id)
                );
            """,
            "create_azure_info_table": """
                CREATE TABLE azure_info(
                    pdf_id INT PRIMARY KEY AUTO_INCREMENT,
                    file_name VARCHAR(255) NOT NULL,
                    title VARCHAR(255) DEFAULT NULL,
                    format varchar(255),
                    creator VARCHAR(255) DEFAULT NULL,
                    author VARCHAR(255) DEFAULT NULL,
                    encryption VARCHAR(20) DEFAULT NULL,
                    number_of_pages INT,
                    number_of_words INT,
                    number_of_images INT,
                    number_of_tables INT
                );
            """,
            "create_azure_page_info_table": """
                CREATE TABLE azure_page_info(
                    info_id INT PRIMARY KEY AUTO_INCREMENT,
                    page_id INT NOT NULL,
                    text TEXT DEFAULT NULL,
                    pdf_id INT NOT NULL,
                    FOREIGN KEY (pdf_id) REFERENCES azure_info(pdf_id),
                    INDEX (page_id)
                );
            """,
            "create_azure_attachment_table": """
                CREATE TABLE azure_attachments(
                    attachment_id INT PRIMARY KEY AUTO_INCREMENT,
                    attachment_name VARCHAR(255) NOT NULL,
                    attachment_url TEXT NOT NULL
                );
            """,
            "create_azure_attachment_mapping_table": """
                CREATE TABLE azure_attachment_mapping(
                    mapping_id INT PRIMARY KEY AUTO_INCREMENT,
                    pdf_id INT NOT NULL,
                    page_id INT NOT NULL,
                    attachment_id INT NOT NULL,
                    FOREIGN KEY (pdf_id) REFERENCES azure_info(pdf_id),
                    FOREIGN KEY (page_id) REFERENCES azure_page_info(page_id),
                    FOREIGN KEY (attachment_id) REFERENCES azure_attachments(attachment_id)
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
    
    cloud_uploader_pymupdf_task = PythonOperator(
        task_id='cloud_uploader_pymupdf',
        python_callable=cloud_uploader_pymupdf
    )
    
    # Task Dependencies
    download_pdf_task >> extract_pymupdf_task >> extract_metadata_task >> setup_tables_task >> cloud_uploader_pymupdf_task