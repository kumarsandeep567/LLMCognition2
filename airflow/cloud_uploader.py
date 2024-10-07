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


def main():
    setup_tables()
    cloud_uploader_pymupdf()


if __name__ == "__main__":
    main()