
# Assignment 2
GAIA Insight: OpenAI Model Evaluation against GAIA dataset
An interactive application built using Streamlit to evaluate the performance of OpenAI GPT Model against the HuggingFace's GAIA(General AI Assistant) dataset. The application extracts content from the PDF files in the GAIA dataset, processes the information, and sends it to assess GPT's ability to provide accurate answers based on the given context of pdf file and annotation metadata (steps to get the correct answer)

## Problem Statement
As Large Language Models (LLMs) like GPT become increasingly prevalent in various applications, it's crucial to assess their performance accurately, especially in specialized domains. The GAIA dataset provides a benchmark for evaluating AI assistants across diverse tasks. This project aims to create a comprehensive tool that allows researchers and developers to evaluate the comprehension capabilities of LLM, specifically OpenAI's GPT-4o. The application primarily focuses on:

1. Automating the data acquisition process for PDF files by creating Airflow pipeline
2. Automatically extract and process information from the GAIA dataset's PDF files
3. Prompt GPT with questions, providing it with extracted content from PDF files and annotation metadata
4. Compare GPT's responses against the known correct answers from the GAIA dataset

## Project Goals
### 1. Airflow Pipelines
- Objective: Streamline the process of retrieving, extracting content and processing a list of PDF files from GAIA benchmarking test and validation datasets with the choosen text extraction tool. Integration of both open-source (ex: PyMuPDF) and API-based (ex: Adobe PDF Extract, Azure AI Document Intelligence) text extraction methods ensuring the extracted information is accurately populated into the data storage (ex: AWS S3, Google Cloud Storage)
- Tools:
   1. Extraction of data from hugging face - huggingface_hub downloader, list_repo_files
   2. Database - Amazon RDS MySQL
   3. File storage - Google Cloud Storage
   4. Open Source PDF Extractor tool - PyMuPDF
   5. API-based PDF Extractor tool - Adobe PDF Extract, Azure AI Document Intelligence
- Output: Extracted data from pdf files is stored in Amazon RDS in formatted manner. All the CSV, Images, JSON files extracted from the PDF using different PDF Extractor tools are stored in their respective folders under the pdf filename in Google Cloud Storage. Extracted text data which is in JSON is formatted into specific tables like pymupdf_info, adobe_info, azure_info. Prompt and annotation data from test and validation datasets are formatted into gaia_features and gaia_annotations table. Users information is being recorded in users table. All the tables are stored in Amazon RDS MySQL Database.

### 2. FastAPI
- Objective:
- Tools:
- Output: 

### 3. Streamlit
- Objective: To provide a user-friendly question answering interface that enables users to ask questions or submit queries. User registration and login interface allows users to create accounts and login securely. Functionalities that allows users to select from a variety of PDF Extract tools either open source or API-based to extract contents from PDF files attached to the question prompt are implemented. The OpenAI answers are compared with the correct answers to evaluate the performance of OpenAI models
- Tools: Streamlit (web application framework), Requests (API calls for data retrieval).
- Output: The Home Page gives an overview of how to use OpenAI Model Evaluation Tool for users like a user-manual, the Login & Registration Page allows users to authenticate their login securely, the Search Engine page allows users to select dataset type, prompt from the list of prompts available, and PDF extraction tool to extract contents from the PDF file, the Validation page validates the OpenAI generated answer with the final answer in the database when the question prompt along with the pdf extracted text is given as prompt to OpenAI GPT model.


### 2. FastAPI
Objective: Implement secure backend services and business logic.
Features:
User registration and login with JWT authentication.
Protected API endpoints (except for registration and login).
Integration with SQL database for user management.
Implementation of business logic and services to be invoked by Streamlit.

### 3. Streamlit
Objective: Provide a user-friendly interface for model evaluation and question answering.
Features:
User registration and login interface.
Question Answering interface with PDF selection capability.
Display of model responses and performance metrics.

4. Deployment
Containerization of FastAPI and Streamlit applications using Docker.
Deployment to a public cloud platform using Docker Compose.
Ensuring public accessibility of the deployed applications.

### 1. Airflow Pipeline
- Objective: Extract data from Hugging Face GAIA benchmark dataset, formatting the textual data, loading it into the database, and loading all files into a bucket
- Tools: For extraction of data from hugging face - huggingface_hub downloader, for storage - MySQL database, for file storage - Google Cloud Storage bucket
- Output: Structured database in MySQL with all gaia_features, and gaia_annotations columns, formatted files stored in the google cloud storage bucket

### 2. Fast API
- Objective: The FastAPI application serves as an abstraction layer that hides all application processing, API calls to OpenAI, fetching data from MySQL database, downloading files from Google Cloud Storage Bucket.
- Tools: MySQL database connector for interfacing with MySQL database, Google Cloud connector for downloading files from Google Cloud, OpenAI package for interacting with GPT (for text based content) and Whisper (for audio based content), tiktoken library for counting the number of tokens, PyPDF2 library for extracting content from PDF files, Base64 library for encoding images to text, openpyxl library for parsing MS Excel spreadsheets, docx library for parsing MS Word documents, dotenv library for setting environment variables, and json library for parsing json content.
- Output: FastAPI ensures the response is always in a JSON format with HTTP status, message (response content), type (data type of response content), and additional fields (if needed)

### 3. Streamlit
- Objective: To provide a user-friendly interface for validating GPT model responses by allowing users to compare generated responses with validation inputs, edit annotator steps, and provide feedback.
- Tools: Streamlit (web application framework), Pandas (data manipulation), Matplotlib (visualization), NumPy (numerical computations), Requests (API calls for data fetching and sending).
- Output: The Search Engine Page features a query input and result display; the Validation Page allows users to compare GPT responses and provide feedback; the Analytics Page presents user data with visualizations of cost efficiency and operational metrics.

## Architecture Diagram

![Architecture Diagram](https://github.com/BigDataIA-Fall2024-TeamB6/Assignment1/raw/main/diagram/llm_cognition.png)

## Google Cloud Storage links
1. GCS bucket link: https://console.cloud.google.com/storage/browser/gaia_benchmark
2. GCS File Storage Path: https://storage.cloud.google.com/gaia_benchmark/files/file_name

## Live Application Link
Streamlit application link: https://bigdataassignment1.streamlit.app/

## Codelabs Link
Codelabs documentation link: https://codelabs-preview.appspot.com/?file_id=https://docs.google.com/document/d/1QsNQheQwEPl2ARqfzTyy77Sx9AKXMdM_8YSEY6-qCbU/edit#6


## Attestation and Team Contribution
**WE ATTEST THAT WE HAVEN’T USED ANY OTHER STUDENTS’ WORK IN OUR ASSIGNMENT AND ABIDE BY THE POLICIES LISTED IN THE STUDENT HANDBOOK**

Name | NUID | Contribution% 
--- | --- | --- |
Sandeep Suresh Kumar | 002841297 | 33% 
Gomathy Selvamuthiah | 002410534 | 33% 
Deepthi Nasika       | 002474582 | 33% 

## Technologies
[![Hugging Face](https://img.shields.io/badge/Hugging%20Face-FD6A2B?style=for-the-badge&logo=huggingface&logoColor=white)](https://huggingface.co/)
[![Python](https://img.shields.io/badge/Python-FFD43B?style=for-the-badge&logo=python&logoColor=blue)](https://www.python.org/)
[![MySQL](https://img.shields.io/badge/MySQL-4479A1?style=for-the-badge&logo=mysql&logoColor=white)](https://www.mysql.com/)
[![GCS](https://img.shields.io/badge/Google%20Cloud%20Storage-FBCC30?style=for-the-badge&logo=googlecloud&logoColor=black)](https://cloud.google.com/storage)
[![FastAPI](https://img.shields.io/badge/FastAPI-005571?style=for-the-badge&logo=fastapi&logoColor=white)](https://fastapi.tiangolo.com/)
[![Docker](https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white)](https://www.docker.com/)
[![OpenAI](https://img.shields.io/badge/OpenAI-000000?style=for-the-badge&logo=openai&logoColor=white)](https://openai.com/)
[![Streamlit](https://img.shields.io/badge/Streamlit-FF4B4B?style=for-the-badge&logo=streamlit&logoColor=white)](https://streamlit.io/)
[![AWS](https://img.shields.io/badge/AWS-232F3E?style=for-the-badge&logo=amazonaws&logoColor=white)](https://aws.amazon.com/)

## Data Source
1. GAIA benchmark dataset: https://huggingface.co/datasets/gaia-benchmark/GAIA

## Prerequisites
Software Installations required for the project
1. Python Environment
A Python environment allows you to create isolated spaces for your Python projects, managing dependencies and versions separately.

2. Libraries
This assignment requires mutliple python libraries for data manipulation, API interactions, and web development like streamlit, huggingface_hub, pandas, matplotlib, mysql-connector-python.
To download all the the required libraries for the project run the following command:
```bash
pip install -r requirements.txt
```
   
3. Visual Studio Code

4. Docker
 Docker allows you to package applications and their dependencies into containers, ensuring consistent environments across different platforms

5. Google Cloud Platform
Google Cloud Storage is used for efficient storage of files

6. AWS
AWS can be utilized for various cloud services, including storage (Amazon S3)


8. Streamlit
Streamlit is an open-source app framework that allows you to create interactive web applications easily.

9. MySQL Database
Relational database management system that allows you to store and manage data efficiently 

## Project Structure
```
Assignment1/
├── backend/
│   ├── .env.example
│   ├── .gitignore
│   ├── helpers.py
│   ├── main.py
│   ├── requirements.txt
├── database_ETL/
│   │   ├── .env.example
│   │   ├── connectDB.py
│   │   ├── fileLoader.py
│   │   ├── jsonParser.py
│   │   ├── main.py
│   │   └── requirements.txt
├── diagram/
│   ├── diagram.py
│   └── llm_cognition.png
├── streamlit/
│   ├── .streamlit/
│   │   ├── DBconnection.py
│   │   └── config.toml
│   ├── analytics.py
│   ├── app.py
│   ├── loginpage.py
│   ├── searchengine.py
│   └── validation.py
├── LICENSE
└── README.md
```
## How to run the application locally
1. Clone the repository
  
