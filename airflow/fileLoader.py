from huggingface_hub import login, list_repo_files, hf_hub_download
from dotenv import load_dotenv
from google.cloud import storage
from google.oauth2 import service_account
import logging
import os

# Logger function
logging.basicConfig(level = logging.INFO, format = '%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_files_into_gcp(repository_id, repository_type, files, bucket_name, creds_file_path, gcp_folder_path):
    logger.info("Inside load_files_into_gcp function")

    try:
        # Creating Google Cloud Storage Client
        creds = service_account.Credentials.from_service_account_file(creds_file_path)
        client = storage.Client(credentials = creds)
        bkt = client.bucket(bucket_name)

        logger.info("Connection to GCS successful")

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
                logger.warning(f"File {file} does not match test or validation categories. Skipping upload.")
                continue

            # Upload to GCS from request response
            blob = bkt.blob(gcs_file_path)
            blob.upload_from_filename(file_data)
            logger.info(f"Uploaded {file} to GCS bucket {bucket_name}")

    except Exception as e:
        logger.error("GCP connection failed")
        raise e


def load_files(access_token, repository_id, repository_type, file_path):

    # Login to hugging face
    login(token = access_token, add_to_git_credential = True)

    # Load all the files from the GAIA benchmark repository
    files = list_repo_files(
        repo_id = repository_id,
        repo_type = repository_type
    )

    # Filter validation files
    total_files = [file for file in files if file.startswith(file_path)]
    return total_files

def main():

    logger.info("Inside main function")
    
    # Load all the environment variables
    load_dotenv()

    access_token = os.getenv("HUGGINGFACE_TOKEN")
    repository_id = os.getenv("REPO_ID")
    repository_type = os.getenv("REPO_TYPE")
    bucket_name = os.getenv("BUCKET_NAME")
    creds_file_path = os.getenv("GCS_CREDENTIALS_PATH")
    file_path = os.getenv("FILE_PATH")
    gcp_folder_path = os.getenv("GCP_FILES_PATH")
    

    # Call load_files function to download all the files from validation set
    files = load_files(access_token, repository_id, repository_type, file_path)
    print(files)
    load_files_into_gcp(repository_id, repository_type, files, bucket_name, creds_file_path, gcp_folder_path)


if __name__ == "__main__":
    try: 
        main()
    except Exception as e:
        logger.error(f"Error while executing main function")
        raise e
    