
# Assignment 2
## OpenAI Model Evaluation Tool with PDF Extraction
An interactive application built using Streamlit to evaluate the performance of OpenAI GPT Model against the HuggingFace's GAIA(General AI Assistant) dataset. The application extracts content from the PDF files in the GAIA dataset, processes the information, and sends it to assess GPT's ability to provide accurate answers based on the given context of pdf file and annotation metadata (steps to get the correct answer)

## Live Application Link
- Streamlit application link: http://18.117.79.65:8501/
- FastAPI: http://18.117.79.65:8000/health

## Codelabs Link
Codelabs documentation link: https://codelabs-preview.appspot.com/?file_id=1f3QFkZMXISlCaRTayBB-mjnfm00do8oNYWJC9lTWMXw#6

## **Video of Submission**
Demo Link: https://youtu.be/advkI-5NLoQ



## Attestation and Team Contribution
**WE ATTEST THAT WE HAVEN’T USED ANY OTHER STUDENTS’ WORK IN OUR ASSIGNMENT AND ABIDE BY THE POLICIES LISTED IN THE STUDENT HANDBOOK**

Name | NUID | Contribution% | Work_Contributed
--- | --- | --- | --- |
Sandeep Suresh Kumar | 002841297 | 33% | PyMuPDF Extractor Tool, FastAPI, Dockerization
Deepthi Nasika       | 002474582 | 33% | Azure PDF Extractor Tool, Streamlit, Airflow Pipeline Setup
Gomathy Selvamuthiah | 002410534 | 33% | Adobe PDF Extractor Tool, Streamlit, Documentation
Ramy Solanki         | 002816593 | 33% | JWT Implementation, DB Schema

## Problem Statement
As Large Language Models (LLMs) like GPT become increasingly prevalent in various applications, it's crucial to assess their performance accurately, especially in specialized domains. The GAIA dataset provides a benchmark for evaluating AI assistants across diverse tasks. This project aims to create a comprehensive tool that allows researchers and developers to evaluate the comprehension capabilities of LLM, specifically OpenAI's GPT-4o. The application primarily focuses on:

1. Automating the data acquisition process for PDF files by creating Airflow pipeline
2. Automatically extract and process information from the GAIA dataset's PDF files
3. Prompt GPT with questions, providing it with extracted content from PDF files and annotation metadata
4. Compare GPT's responses against the known correct answers from the GAIA dataset


## Architecture Diagram
### 1. Airflow ETL Pipeline
![Architecture Diagram](https://github.com/BigDataIA-Fall2024-TeamB6/Assignment2/blob/airflow/diagram/airflow_etl_pipeline.png)

- Automate the data acquisition process for PDF files in the GAIA dataset
- Processing list of PDF files from GAIA benchmarking validation & test datasets
- Integrating it with the PDF Extractor tools either open source or API-based into the pipeline for efficient text extraction

### 2. Core Application
![Architecture Diagram](https://github.com/BigDataIA-Fall2024-TeamB6/Assignment2/blob/airflow/diagram/core_application_service.png)

- Airflow  pipeline streamlining the process of retrieving & processing documents, ensuring the extracted information is stored securely in the cloud Database and files are structurally formatted and stored onto the S3 path
- User Registration & Login functionality, API endpoints with JWT authentication tokens
- User data with their credentials, and hashed passwords are stored in the Database
- All the APIs respective to services are created with authentication in FastAPI
- User-friendly Streamlit application with Question Answering Interface

## Project Goals
### Airflow Pipelines
#### 1. Objective
- Streamline the process of retrieving, extracting content and processing a list of PDF files from GAIA benchmarking test and validation datasets with the choosen text extraction tool.
- Integration of both open-source (ex: PyMuPDF) and API-based (ex: Adobe PDF Extract, Azure AI Document Intelligence) text extraction methods ensuring the extracted information is accurately populated into the data storage (ex: AWS S3, Google Cloud Storage)
#### 2. Tools
- Extraction of data from hugging face - huggingface_hub downloader, list_repo_files
- Database - Amazon RDS MySQL
- File storage - Google Cloud Storage
- Open Source PDF Extractor tool - PyMuPDF
- API-based PDF Extractor tool - Adobe PDF Extract, Azure AI Document Intelligence
#### 3. Output
- Extracted data from pdf files is stored in Amazon RDS in a formatted manner. All the CSV, Images, JSON files extracted from the PDF using different PDF Extractor tools are stored in their respective folders under the pdf filename in Google Cloud Storage.
- Extracted text data which is in JSON is formatted into specific tables like pymupdf_info, adobe_info, azure_info. Prompt and annotation data from test and validation datasets are formatted into gaia_features and gaia_annotations table. Users information is being recorded in users table. All the tables are stored in Amazon RDS MySQL Database.

### FastAPI
#### 1. Objective
- Provide a secure backend service to act as a mediator between the Streamlit application, Database service (AWS RDS), Cloud Storage Services (GCS Bucket), and OpenAI GPT-4o model, whilst implementing accurate authentication and authorization protocols, and providing responses in a streamlined JSON format

#### 2. Tools
- `fastapi[standard]` for building a standard FastAPI application
- `python-multipart` for installing additional dependencies for FastAPI application
- `mysql-connector-python` for interacting with any MySQL database, in this case, AWS Relational Database Service (RDS)
- `PyJWT` for authenticating and authorizing users with JSON Web Tokens (JWT)
- `google-auth` and `google-cloud-storage` for interacting with unstructured objects (like PDF documents) on the Google Cloud Storage
- `openai` for prompting OpenAI's GPT-4o model
- `tiktoken` for disintegrating prompts into tokens of known sizes


#### 3. Output
FastAPI provides a number of endpoints for interacting with the service:
- `GET` - `/health` - To check if the FastAPI application is setup and running
- `GET` - `/database` - To check if FastAPI can communicate with the database
- `POST` - `/register` - To sign up new users to the service
- `POST` - `/login` - To sign in existing users
- `GET` - `/listprompts` - *Protected* - To fetch 'x' number of prompts of type 'type' from the database 
- `GET` - `/loadprompt/{task_id}` - *Protected* - To load all information from the database regarding the given prompt 
- `GET` - `/getannotation/{task_id}` - *Protected* - To load the annotation from the database regarding the given prompt
- `POST` - `/querygpt` - *Protected* - To forward the question to OpenAI GPT4 and evaluate based on GAIA Benchmark
- `GET` - `/feedback` - *Protected* - To save the user's feedback for GPT's response for the task_id
- `POST` - `/markcorrect` - *Protected* - To mark the GPT's response as correct in case minor formatting issues occur

FastAPI ensures that every response is returned in a consistent JSON format with HTTP status, type (data type of the response content) message (response content), and additional fields if needed



### Streamlit
#### 1. Objective
- To provide a user-friendly question answering interface that enables users to ask questions or submit queries. User registration and login interface allows users to create accounts and login securely.
- Functionalities that allows users to select from a variety of PDF Extract tools either open source or API-based to extract contents from PDF files attached to the question prompt are implemented. The OpenAI answers are compared with the correct answers to evaluate the performance of OpenAI models

#### 2. Tools
- Streamlit (web application framework), Requests (API calls for data retrieval)

#### 3. Output
- Home Page gives an overview of how to use OpenAI Model Evaluation Tool for users like a user-manual,
- Login & Registration Page allows users to authenticate their login securely,
- Search Engine page allows users to select dataset type, prompt from the list of prompts available, and PDF extraction tool to extract contents from the PDF file,
- Validation page validates the OpenAI generated answer with the final answer in the database when the question prompt along with the pdf extracted text is given as prompt to OpenAI GPT model.


### Deployment
- Containerization of FastAPI and Streamlit applications using Docker. Deployment to a public cloud platform using Docker Compose. Ensuring public accessibility of the deployed applications.
- The FastAPI and Streamlit are containerized using Docker, and orchestrated through docker compose and the Docker images are pushed to Docker Hub. For deploying the Docker containers, we use an Amazon Web Services (AWS) EC2 instance within the t3-medium tier, followed by the database being hosted on AWS Relational Database Service (RDS). 


## Data Source
1. GAIA benchmark dataset: https://huggingface.co/datasets/gaia-benchmark/GAIA

## Google Cloud Storage links
1. GCS bucket link: https://console.cloud.google.com/storage/browser/gaia_benchmark2
2. GCS File Storage Path: https://console.cloud.google.com/storage/browser/gaia_benchmark2/files
3. GCS Adobe FilePath: https://console.cloud.google.com/storage/browser/gaia_benchmark2/adobe_doc_extract
4. GCS Azure FilePath: https://console.cloud.google.com/storage/browser/gaia_benchmark2/azure_doc_extract
5. GCS PyMuPDF FilePath: https://console.cloud.google.com/storage/browser/gaia_benchmark2/pymupdf_doc_extract

## Technologies
[![Hugging Face](https://img.shields.io/badge/Hugging%20Face-FD6A2B?style=for-the-badge&logo=huggingface&logoColor=white)](https://huggingface.co/)
[![Python](https://img.shields.io/badge/Python-FFD43B?style=for-the-badge&logo=python&logoColor=blue)](https://www.python.org/)
[![GCS](https://img.shields.io/badge/Google%20Cloud%20Storage-FBCC30?style=for-the-badge&logo=googlecloud&logoColor=black)](https://cloud.google.com/storage)
[![FastAPI](https://img.shields.io/badge/FastAPI-005571?style=for-the-badge&logo=fastapi&logoColor=white)](https://fastapi.tiangolo.com/)
[![Docker](https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white)](https://www.docker.com/)
[![OpenAI](https://img.shields.io/badge/OpenAI-000000?style=for-the-badge&logo=openai&logoColor=white)](https://openai.com/)
[![Streamlit](https://img.shields.io/badge/Streamlit-FF4B4B?style=for-the-badge&logo=streamlit&logoColor=white)](https://streamlit.io/)
[![Airflow](https://img.shields.io/badge/Airflow-17B3A8?style=for-the-badge&logo=apacheairflow&logoColor=white)](https://airflow.apache.org/)
[![Adobe PDF Extract](https://img.shields.io/badge/Adobe%20PDF%20Extract-FF2B2B?style=for-the-badge&logo=adobe&logoColor=white)](https://www.adobe.com/analytics/pdfs/adobe-pdf-extract.pdf)
[![Azure AI Document Intelligence](https://img.shields.io/badge/Azure%20AI%20Document%20Intelligence-0078D4?style=for-the-badge&logo=microsoftazure&logoColor=white)](https://azure.microsoft.com/en-us/services/cognitive-services/document-intelligence/)
[![PyMuPDF](https://img.shields.io/badge/PyMuPDF-333333?style=for-the-badge&logo=python&logoColor=white)](https://pymupdf.readthedocs.io/en/latest/)
[![Postman](https://img.shields.io/badge/Postman-FF6C37?style=for-the-badge&logo=postman&logoColor=white)](https://www.postman.com/)
[![Amazon RDS](https://img.shields.io/badge/Amazon%20RDS-527FFF?style=for-the-badge&logo=amazonaws&logoColor=white)](https://aws.amazon.com/rds/)


## Prerequisites
Software Installations required for the project
1. Python Environment
A Python environment allows you to create isolated spaces for your Python projects, managing dependencies and versions separately

2. Poetry Environment
Poetry is a dependency management tool that helps you manage your Python packages and projects efficiently where a user can install all the dependencies onto pyproject.toml file

3. Packages
The project requires multiple packages for loading environment variables: python-dotenv, for loading files from hugging face: huggingface-hub, for connecting to MySQL database: mysql-connector-python, for file storage: google-cloud-storage, for extracting PDF contents from Azure AI Document Intelligence tool: azure-ai-formrecognizer, for extracting PDF contents from Adobe PDF Extract pdfservices-sdk, for extracting PDF contents with open source tool: pymupdf
```bash
pip install -r requirements.txt
```

4. Visual Studio Code
An integrated development environment (IDE) that provides tools and features for coding, debugging, and version control.

5. Docker
 Docker allows you to package applications and their dependencies into containers, ensuring consistent environments across different platforms. All the dependencies will be installed on docker-compose.yaml file with env file

6. Google Cloud Storage
Google Cloud Storage is used for efficient storage of files. All the files loaded from Hugging Face are downloaded to GCS bucket. The extracted contents from the pdf files by all the 3 different PDF Extractor tools like PyMuPDF, Adobe PDF Extract, Azure AI Document Intelligence Tool which are organized into seperate folders like CSV, JSON, Images are also stored into GCS bucket

8. Streamlit
Streamlit is an open-source app framework that allows you to create interactive web applications easily.

9. Amazon RDS
Amazon RDS is a managed relational database service that makes it easy to set up, operate, and scale a relational database in the cloud. Supports multiple database engines including MySQL, PostgreSQL, Oracle, and SQL Server. 

## Project Structure
```
Assignment2/
├── airflow/
│   ├── .env.example
│   ├── airflow_pipeline.py
│   ├── azure_pdfFileExtractor.py
│   ├── cloud_uploader.py
│   ├── docker-compose.yaml
│   ├── fileLoader.py
│   ├── fileParser.py
│   ├── pymupdf_content_extractor.py
│   ├── requirements.txt
├── diagram/
│   ├──images/
│   │   ├── Adobe.png
│   │   ├── Azure.png
│   │   ├── CSV.png
│   │   ├── HuggingFace_logo.png
│   │   ├── JSON.png
│   │   ├── JSON_CSV_PNG.png
│   │   ├── OpenAI.png
│   │   ├── PDF_documents.png
│   │   ├── PNG.png
│   │   └── PyMuPDF.png
│   ├── airflow_architecture.py
│   ├── airflow_etl_pipeline.png
│   ├── core_app_architecture.py
│   ├── core_application_service.png
│   └── requirements.txt
├── fastapi/
│   ├── .env.example
│   ├── helpers.py
│   ├── main.py
│   └── requirements.txt
├── streamlit/
│   ├── .streamlit/
│   │   ├── DBconnection.py
│   │   └── config.toml
│   ├── .env.example
│   ├── app.py
│   ├── homepage.py
│   ├── loginpage.py
│   ├── overview.py
│   ├── registerpage.py
│   ├── searchengine.py
│   ├── validation.py
│   └── requirements.txt
├── .gitignore
├── LICENSE
└── README.md

```

## How to run the application locally
1. **Clone the Repository**: Clone the repository onto your local machine and navigate to the directory within your terminal.

   ```bash
   git clone https://github.com/BigDataIA-Spring2024-Sec1-Team4/Assignment2
   ```

2. **Install Docker**: Install docker and `docker compose` to run the application:

   - For Windows, Mac OS, simply download and install Docker Desktop from the official website to install docker and `docker compose` 
   [Download Docker Desktop](https://www.docker.com/products/docker-desktop/)

   - For Linux (Ubuntu based distributions), 
   ```bash
   # Add Docker's official GPG key:
   sudo apt-get update
   sudo apt-get install ca-certificates curl
   sudo install -m 0755 -d /etc/apt/keyrings
   sudo curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
   sudo chmod a+r /etc/apt/keyrings/docker.asc

   # Add the repository to Apt sources:
   echo \
   "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu \
   $(. /etc/os-release && echo "$VERSION_CODENAME") stable" | \
   sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
   sudo apt-get update 

   # Install packages for Docker
   sudo apt-get install docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin

   # Check to see if docker is running 
   sudo docker run hello-world

3. **Run the application:** In the terminal within the directory, run 
   ```bash
   docker-compose up

   # To run with logging disabled, 
   docker-compose up -d

4. In the browser, 
   - visit `localhost:8501` to view the Streamlit application
   - visit `localhost:8000/docs` to view the FastAPI endpoint docs
