import os
import time
import json
import logging
import mysql.connector
from dotenv import load_dotenv
from mysql.connector import Error
from google.cloud import storage
from google.oauth2 import service_account

# Load the environment variables
load_dotenv()

# ============================= Logger : Begin =============================

# Initialize logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

# Log to console (dev only)
if os.getenv('APP_ENV', "development") == "development":
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    logger.addHandler(handler)

# Also log to a file
file_handler = logging.FileHandler(os.getenv('PYMUPDF_UPLOAD_LOG_FILE', 'cloud_uploader_pymupdf.log'))
file_handler.setFormatter(formatter)
logger.addHandler(file_handler) 

# ============================= Logger : End ===============================

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
            logger.info("DATABASE - create_connection() - Connection to the database was opened")
            return conn
        
        except (Error, IOError) as error:
            if attempt == attempts:
                # Ran out of attempts
                logger.error(f"DATABASE - create_connection() - Failed to connect to database : {error}")
                return None
            else:
                logger.warning(f"DATABASE - create_connection() - Connection failed: {error} - Retrying {attempt}/{attempts} ...")
                
                # Delay the next attempt
                time.sleep(delay ** attempt)
                attempt += 1
    
    return None


def setup_tables() -> None:
    '''Drop existing tables, if any, and create the tables in the database'''

    logger.info("DATABASE - setup_tables() - Request to setup tables received")

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
                    logger.info(f"SQL - setup_tables() - Running query for {table_name}")

                    cursor.execute(query)
                    conn.commit()

                    logger.info(f"SQL - setup_tables() - Query executed successfully for {table_name}")
    
    except Exception as exception:
        logger.error("DATABASE - setup_tables() - Error occured while setting up the tables")
        logger.error(exception)

    finally:
        if conn and conn.is_connected():
            conn.close()
            logger.info("DATABASE - setup_tables() - Connection to the database was closed")


def cloud_uploader_pymupdf() -> None:
    '''Upload the contents extracted by PyMuPDF to Database and Google Cloud Storage Bucket'''

    logger.info("AIRFLOW - cloud_uploader_pymupdf() - Request to upload contents to database and cloud received")

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
        logger.error(f"AIRFLOW - cloud_uploader_pymupdf() - Error fetching directory contents: {base_dir}")
        logger.error(exception)

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
                    logger.info(f"SQL - cloud_uploader_pymupdf() - Inserting metadata contents for file {directory}")

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
                    logger.info(f"SQL - cloud_uploader_pymupdf() - Metadata inserted for file {directory}")

                    # Upload the metadata file to the bucket
                    logger.info(f"GCP - cloud_uploader_pymupdf() - Uploading metadata file to GCS Bucket for file {directory}")
                    
                    try:
                        metadata_blob = f"{bucket_storage_dir}/{directory}/" + os.path.basename(metadata_file_path)
                        blob= bucket.blob(metadata_blob)
                        blob.upload_from_filename(metadata_file_path)
                        logger.info(f"GCP - cloud_uploader_pymupdf() - Uploaded metadata file to GCS Bucket for file {directory}")
                    
                    except Exception as exception:
                        logger.error(f"GCP - cloud_uploader_pymupdf() - Error occured while uploading metadata file to GCS Bucket for file {directory}")
                        logger.error(exception)

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
                                        logger.info(f"GCP - cloud_uploader_pymupdf() - Uploading {folder} directory and its contents to GCS Bucket for file {directory}")
                                        folder_blob = f"{bucket_storage_dir}/{directory}/{folder}/{file}"
                                        blob = bucket.blob(folder_blob)
                                        blob.upload_from_filename(os.path.join(folder_dir, file))
                                else:
                                    logger.info(f"GCP - cloud_uploader_pymupdf() - Uploading empty {folder} directory to GCS Bucket for file {directory}")
                                    folder_blob = f"{bucket_storage_dir}/{directory}/{folder}/"
                                    blob = bucket.blob(folder_blob)
                                    blob.upload_from_string('')
                                
                                logger.info(f"GCP - cloud_uploader_pymupdf() - Uploaded {folder} directory to GCS Bucket for file {directory}")
                            else:
                                logger.warning(f"GCP - cloud_uploader_pymupdf() - {folder} directory does not exist for file {directory}")

                    except Exception as exception:
                        logger.error(f"AIRFLOW - cloud_uploader_pymupdf() - Error occurred while uploading directories to GCP Bucket for {directory}")
                        logger.error(exception)


                    ################## Load page content into the database ##################

                    # Fetch the pdf_id 
                    logger.info(f"SQL - cloud_uploader_pymupdf() - Fetching pdf_id from database for file {directory}")
                        
                    cursor.execute(f"SELECT `pdf_id` FROM pymupdf_info WHERE file_name = '{str(directory)}'")
                    pdf_id = cursor.fetchone()[0]

                    logger.info(f"SQL - cloud_uploader_pymupdf() - Fetched pdf_id from database for file {directory}")
                    json_file_list = []

                    try:
                        json_dir = os.path.join(base_dir, directory, 'JSON')
                        json_file_list = os.listdir(json_dir)
                        
                    except Exception as exception:
                        json_file_list = None
                        logger.error(f"AIRFLOW - cloud_uploader_pymupdf() - Error fetching directory contents: {json_dir}")
                        logger.error(exception)
                        
                    if json_file_list is not None:
                        for jsonFile in json_file_list:
                            
                            page_file_path = os.path.join(json_dir, jsonFile)
                            logger.info(f"AIRFLOW - cloud_uploader_pymupdf() - Processing {directory}/JSON/{jsonFile}")
                            
                            with open(page_file_path, 'r') as _file:
                                page = json.load(_file)

                            # Read the 'text' content in each json file, and save them to the database
                            insert_text_query= """
                            INSERT INTO pymupdf_page_info (page_id, pdf_id, text)
                            VALUES (%s, %s, %s)
                            """

                            logger.info(f"SQL - cloud_uploader_pymupdf() - Inserting text for pdf_id {pdf_id}")
                            cursor.execute(insert_text_query, (
                                page['page_id'],
                                pdf_id, 
                                page['content']['text']
                            ))

                            conn.commit()
                            logger.info(f"SQL - cloud_uploader_pymupdf() - Inserted text for pdf_id {pdf_id}")

                            # Read the 'table', 'image' content in each json file, link them in the attachments and mappings table
                            items = ['table', 'image']

                            for item in items:
                                if len(page['content'][item]) > 0:
                                    for file_name in page['content'][item]:

                                        logger.info(f"SQL - cloud_uploader_pymupdf() - Inserting {item} record {file_name} for pdf_id {pdf_id}")
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
                                        logger.info(f"SQL - cloud_uploader_pymupdf() - Inserted {item} record {file_name} for pdf_id {pdf_id}")
                                        
                                        # Linking attachment, page, and pdf in the mappings table
                                        
                                        # Fetch the attachment_id for the recently inserted attachment
                                        logger.info(f"SQL - cloud_uploader_pymupdf() - Fetching attachment_id for {file_name} for file {directory}")
                                        cursor.execute(f"SELECT `attachment_id` FROM pymupdf_attachments ORDER BY `attachment_id` DESC LIMIT 1")
                                        attachment_id = cursor.fetchone()[0]
                                        logger.info(f"SQL - cloud_uploader_pymupdf() - Fetched attachment_id for {file_name} for file {directory}")
                                        
                                        # Insert the mapping into the mappings table
                                        logger.info(f"SQL - cloud_uploader_pymupdf() - Inserting attachment_mapping for {file_name} for page_id {page['page_id']} for pdf_id {pdf_id}")
                                        
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
                                        logger.info(f"SQL - cloud_uploader_pymupdf() - Inserting attachment_mapping for {file_name} for page_id {page['page_id']} for pdf_id {pdf_id}")

            except Exception as exception:
                logger.error("AIRFLOW - cloud_uploader_pymupdf() - Error occured while inserting data into database")
                logger.error(exception)

            finally:
                if conn and conn.is_connected():
                    conn.close()
                    logger.info("DATABASE - cloud_uploader_pymupdf() - Connection to the database was closed")

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
    
    except Exception as e:
        logger.error("Azure - cloud_uploader_azure() - Error while executing cloud_uploader_azure() function")
        raise e

    finally:
        conn.close()
        logger.info("Azure - cloud_uploader_azure() - Connection to the database closed")
        logger.info("Azure - cloud_uploader_azure() - Uploading content to database and GCS successful")

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

def load_parsed_data_to_db():
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

def main():
    setup_tables()
    cloud_uploader_pymupdf()
    cloud_uploader_azure()
    load_parsed_data_to_db()


if __name__ == "__main__":
    main()