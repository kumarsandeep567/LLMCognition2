
# Assignment 2
GAIA Insight: OpenAI Model Evaluation against GAIA dataset
An interactive application built using Streamlit to evaluate the performance of OpenAI GPT Model against the HuggingFace's GAIA(General AI Assistant) dataset. The application extracts content from the PDF files in the GAIA dataset, processes the information, and sends it to assess GPT's ability to provide accurate answers based on the given context of pdf file and annotation metadata (steps to get the correct answer)

## Problem Statement
As Large Language Models (LLMs) like GPT become increasingly prevalent in various applications, it's crucial to assess their performance accurately, especially in specialized domains. The GAIA dataset provides a benchmark for evaluating AI assistants across diverse tasks. This project aims to create a comprehensive tool that allows researchers and developers to evaluate the comprehension capabilities of LLM, specifically OpenAI's GPT-4o. The application primarily focuses on:

1. Automating the data acquisition process for PDF files by creating Airflow pipeline
2. Automatically extract and process information from the GAIA dataset's PDF files
3. Prompt GPT with questions, providing it with extracted content from PDF files and annotation metadata
4. Compare GPT's responses against the known correct answers from the GAIA dataset

## Live Application Link
Streamlit application link: https://bigdataassignment1.streamlit.app/

## Codelabs Link
Codelabs documentation link: https://codelabs-preview.appspot.com/?file_id=1f3QFkZMXISlCaRTayBB-mjnfm00do8oNYWJC9lTWMXw#6

## Attestation and Team Contribution
**WE ATTEST THAT WE HAVEN’T USED ANY OTHER STUDENTS’ WORK IN OUR ASSIGNMENT AND ABIDE BY THE POLICIES LISTED IN THE STUDENT HANDBOOK**

Name | NUID | Contribution% 
--- | --- | --- |
Sandeep Suresh Kumar | 002841297 | 33% 
Gomathy Selvamuthiah | 002410534 | 33% 
Deepthi Nasika       | 002474582 | 33% 


## 1. Architecture Diagram - Airflow ETL Pipeline
![Architecture Diagram](https://github.com/BigDataIA-Fall2024-TeamB6/Assignment2/blob/airflow/diagram/airflow_etl_pipeline.png)

- Automate the data acquisition process for PDF files in the GAIA dataset
- Processing list of PDF files from GAIA benchmarking validation & test datasets
- Integrating it with the PDF Extractor tools either open source or API-based into the pipeline for efficient text extraction

## Architecture Diagram - Core Application
![Architecture Diagram](https://github.com/BigDataIA-Fall2024-TeamB6/Assignment2/blob/airflow/diagram/core_application_service.png)

- Airflow  pipeline streamlining the process of retrieving & processing documents, ensuring the extracted information is stored securely in the cloud Database and files are structurally formatted and stored onto S3 path
- User Registration & Login functionality, API endpoints with JWT authentication tokens
- User data with their credentials, hashed passwords are stored into the Database
- All the APIs respective to services are created with authentication in FastAPI
- User-friendly Streamlit application with Question Answering Interface

## Project Goals
### 1. Airflow Pipelines
#### Objective
1. Streamline the process of retrieving, extracting content and processing a list of PDF files from GAIA benchmarking test and validation datasets with the choosen text extraction tool.
2. Integration of both open-source (ex: PyMuPDF) and API-based (ex: Adobe PDF Extract, Azure AI Document Intelligence) text extraction methods ensuring the extracted information is accurately populated into the data storage (ex: AWS S3, Google Cloud Storage)
#### Tools
1. Extraction of data from hugging face - huggingface_hub downloader, list_repo_files
2. Database - Amazon RDS MySQL
3. File storage - Google Cloud Storage
4. Open Source PDF Extractor tool - PyMuPDF
5. API-based PDF Extractor tool - Adobe PDF Extract, Azure AI Document Intelligence
#### Output
1. Extracted data from pdf files is stored in Amazon RDS in formatted manner. All the CSV, Images, JSON files extracted from the PDF using different PDF Extractor tools are stored in their respective folders under the pdf filename in Google Cloud Storage.
2. Extracted text data which is in JSON is formatted into specific tables like pymupdf_info, adobe_info, azure_info. Prompt and annotation data from test and validation datasets are formatted into gaia_features and gaia_annotations table. Users information is being recorded in users table. All the tables are stored in Amazon RDS MySQL Database.

### 2. FastAPI
#### Objective

#### Tools

#### Output

Objective: Implement secure backend services and business logic.
Features:
User registration and login with JWT authentication.
Protected API endpoints (except for registration and login).
Integration with SQL database for user management.
Implementation of business logic and services to be invoked by Streamlit.

- Objective: The FastAPI application serves as an abstraction layer that hides all application processing, API calls to OpenAI, fetching data from MySQL database, downloading files from Google Cloud Storage Bucket.
- Tools: MySQL database connector for interfacing with MySQL database, Google Cloud connector for downloading files from Google Cloud, OpenAI package for interacting with GPT (for text based content) and Whisper (for audio based content), tiktoken library for counting the number of tokens, PyPDF2 library for extracting content from PDF files, Base64 library for encoding images to text, openpyxl library for parsing MS Excel spreadsheets, docx library for parsing MS Word documents, dotenv library for setting environment variables, and json library for parsing json content.
- Output: FastAPI ensures the response is always in a JSON format with HTTP status, message (response content), type (data type of response content), and additional fields (if needed)



### 3. Streamlit
#### Objective
1. To provide a user-friendly question answering interface that enables users to ask questions or submit queries. User registration and login interface allows users to create accounts and login securely.
2. Functionalities that allows users to select from a variety of PDF Extract tools either open source or API-based to extract contents from PDF files attached to the question prompt are implemented. The OpenAI answers are compared with the correct answers to evaluate the performance of OpenAI models

#### Tools
- Streamlit (web application framework), Requests (API calls for data retrieval)

#### Output
1. Home Page gives an overview of how to use OpenAI Model Evaluation Tool for users like a user-manual,
2. Login & Registration Page allows users to authenticate their login securely,
3. Search Engine page allows users to select dataset type, prompt from the list of prompts available, and PDF extraction tool to extract contents from the PDF file,
4. Validation page validates the OpenAI generated answer with the final answer in the database when the question prompt along with the pdf extracted text is given as prompt to OpenAI GPT model.


### Deployment
Containerization of FastAPI and Streamlit applications using Docker.
Deployment to a public cloud platform using Docker Compose.
Ensuring public accessibility of the deployed applications.

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
 Docker allows you to package applications and their dependencies into containers, ensuring consistent environments across different platforms

6. Google Cloud Storage
Google Cloud Storage is used for efficient storage of files. All the files loaded from Hugging Face are downloaded to GCS bucket. The extracted contents from the pdf files which are organized into seperate folders like CSV, JSON, Images are also stored into GCS bucket


8. Streamlit
Streamlit is an open-source app framework that allows you to create interactive web applications easily.

9. Amazon RDS
Relational database management system that allows you to store and manage data efficiently 

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
1. Clone the repository
  
