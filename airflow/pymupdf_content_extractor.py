import os
import json
import shutil
import logging
import pymupdf
import pandas as pd
from dotenv import load_dotenv
from unidecode import unidecode
from huggingface_hub import hf_hub_download, list_repo_files


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
file_handler = logging.FileHandler(os.getenv('PYMUPDF_EXTRACT_LOG_FILE', 'content_extractor_pymupdf.log'))
file_handler.setFormatter(formatter)
logger.addHandler(file_handler) 

# ============================= Logger : End ===============================


def pdf_downloader() -> bool:
    '''Download PDF files from a HuggingFace repository and save them locally in respective directories'''

    # Set the parameters 
    access_token        = os.getenv("HUGGINGFACE_TOKEN", None)
    repository_id       = os.getenv("REPO_ID", None)
    repository_type     = os.getenv("REPO_TYPE", None)
    hf_directory_path   = os.getenv("DIRECTORY_PATH", None)

    logger.info("AIRFLOW - pdf_downloader() - Request to download PDF files received")
    status = False

    # Validate that all required inputs are provided
    if access_token is None:
        logger.error("AIRFLOW - pdf_downloader() - HuggingFace token was expected but received None")
        return status

    if repository_id is None:
        logger.error("AIRFLOW - pdf_downloader() - HuggingFace Repository ID was expected but received None")
        return status

    if repository_type is None:
        logger.error("AIRFLOW - pdf_downloader() - HuggingFace Repository Type was expected but received None")
        return status

    if hf_directory_path is None:
        logger.error("AIRFLOW - pdf_downloader() - Directory path for HuggingFace Repository was expected but received None")
        return status

    try:

        # List files from the HuggingFace repository
        logger.info("AIRFLOW - pdf_downloader() - Fetching file list from HuggingFace")
        dataset_file_list = list_repo_files(
            token       = access_token,
            repo_id     = repository_id,
            repo_type   = repository_type
        )
        logger.info("AIRFLOW - pdf_downloader() - File list fetched from HuggingFace")

        if len(dataset_file_list) > 0:
            try:
                # Remove the directories if they exist
                if os.path.isdir('2023'):
                    shutil.rmtree('2023')
            
            except OSError as exception:
                logger.error("AIRFLOW - pdf_downloader() - Error removing directories")
                logger.error(exception)
                return status

            # Download PDF files and save them in appropriate directories
            logger.info("AIRFLOW - pdf_downloader() - Downloading PDF files from HuggingFace")
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
                        logger.error(f"AIRFLOW - pdf_downloader() - Error downloading file {file}")
                        logger.error(exception)

            # Everything worked hopefully
            status = True
            logger.info("AIRFLOW - pdf_downloader() - PDF files downloaded successfully")
        
        else:
            logger.error("AIRFLOW - pdf_downloader() - Zero files were found in the repository. Are the repository details correct?")
    
    except Exception as exception:
        logger.error("AIRFLOW - pdf_downloader() - Error accessing HuggingFace repository")
        logger.error(exception)
    
    return status


def get_pdf_list():
    '''Create a list of PDFs (with absolute paths) to parse'''

    logger.info("AIRFLOW - get_pdf_list() - Request received to create a list of PDF files")
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

        logger.info("AIRFLOW - get_pdf_list() - Creation of a list of PDF files completed")

    except Exception as exception:
        pdf_list = None
        logger.error("AIRFLOW - get_pdf_list() - Error fetching directory contents")
        logger.error(exception)
    
    return pdf_list


def extract_content_pymupdf():
    '''Extract the contents of the PDF and store the contents in JSON and CSV formats, wherever needed'''

    logger.info("AIRFLOW - extract_content_pymupdf() - Request received to extract PDF contents through PyMuPDF")

    try:
        # Remove the directory if it exists
        if os.path.isdir('extracted_contents'):
            shutil.rmtree('extracted_contents')

    except OSError as exception:
        logger.error("AIRFLOW - extract_content_pymupdf() - Error removing directories")
        logger.error(exception)

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
                                    logger.error(f"Error extracting image on Page {page_id} of PDF {pdf_path}")
                                    logger.error(exception)
                            
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
                                    logger.error(f"Error extracting table on Page {page_id} of PDF {pdf_path}")
                                    logger.error(exception)

                        
                            page_content['content']['table'] = table_list
                            
                            # Save page content as JSON
                            json_file_path = os.path.join(json_dir, f"{page_id}.json")
                            with open(json_file_path, 'w') as json_file:
                                json.dump(page_content, json_file, indent=4)
                        
                        except Exception as exception:
                            logger.error(f"AIRFLOW - extract_content_pymupdf() - Error occured while processing Page {page_id+1} of PDF {pdf_path}")
                            logger.error(exception)

            except Exception as exception:
                logger.error("AIRFLOW - extract_content_pymupdf() - Failed to open the PDF document")
                logger.error(exception)

        logger.info("AIRFLOW - get_pdf_list() - Content extraction through PyMuPDF complete")


def extract_metadata():
    """Extracts metadata including word count, image count, and table count for each PDF."""

    logger.info("AIRFLOW - extract_metadata() - Request received to create metadata file for PDFs through PyMuPDF")

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
                    logger.error(f"JSON directory for {pdf_name} not found")
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
            logger.error(f"AIRFLOW - extract_metadata() - Error occured while processing PDF {pdf_name}")
            logger.error(exception)

    logger.info("AIRFLOW - extract_metadata() - Metadata extraction through PyMuPDF complete")
    

def main():

    # Download the PDF files
    download_status = pdf_downloader()

    if download_status:
        extract_content_pymupdf()
        extract_metadata()

if __name__ == "__main__":
    main()