from diagrams import Diagram, Cluster, Edge
from diagrams.onprem.workflow import Airflow
from diagrams.gcp.storage import Storage
from diagrams.aws.database import RDS
from diagrams.custom import Custom

with Diagram("Airflow ETL Pipeline", show=False):
    
    with Cluster("Data Source"):
        hugging_face_data = Custom("Hugging Face\nGAIA Dataset\n", "./images/HuggingFace_logo.png")

    with Cluster("Content Extraction Service", direction="TB"):
        pymupdf = Custom("PyMuPDF\n(Open Source)", "./images/PyMuPDF.png")
        adobe_extract = Custom("Adobe PDF Extract API\n(Proprietary)", "./images/Adobe.png")
        azure_doc = Custom("Azure AI Document Intelligence\n(Proprietary)", "./images/Azure.png")

    with Cluster("Data Loading", direction="TB"):
        cloud_uploader1 = Airflow("Cloud Uploader\n(Airflow Function)")
        cloud_uploader2 = Airflow("Cloud Uploader\n(Airflow Function)")

    # Define Nodes
    hugging_face_downloader = Airflow("HF File Downloader\n(Airflow Function)")
    pdf_docs = Custom("PDF documents", "./images/PDF_documents.png")
    fileLoader = Airflow("File Loader\n(Airflow Function)")
    fileParser = Airflow("File Parser\n(Airflow Function)")
    content_handler = Airflow("Content Handler\n(Airflow Function)")
    setupTables = Airflow("Setup Tables\n(Airflow Function)")
    extracted_contents = Custom("Extracted Contents", "./images/JSON_CSV_PNG.png")
    gcp_sql = RDS("AWS RDS\n(Text, JSON, \nUser data, Analytics)")
    gcp_storage = Storage("Google Cloud Storage Bucket\n(CSV, Images, Audio, Video)")

    # Define connections
    hugging_face_data >> Edge(color="black", style="solid") >> hugging_face_downloader 
    fileLoader >> Edge(color="black", style="solid") >> gcp_storage
    fileParser >> Edge(color="black", style="solid") >> gcp_storage
    setupTables >> Edge(color="black", style="solid") >> gcp_sql
    hugging_face_downloader >> Edge(color="black", style="solid") >> pdf_docs 
    pdf_docs >> Edge(color="black", style="solid") >> content_handler
    content_handler >> Edge(color="black", style="solid") >> extracted_contents 
    
    # Connect services
    content_handler << Edge(color="black", style="solid") >> pymupdf

    content_handler << Edge(color="black", style="solid", label="REST API") >> adobe_extract
    
    content_handler << Edge(color="black", style="solid") >> azure_doc

    extracted_contents >> Edge(color="black", style="solid") >> cloud_uploader1 
    cloud_uploader1 >> Edge(color="black", style="solid") >> gcp_sql
    
    extracted_contents >> Edge(color="black", style="solid") >> cloud_uploader2 
    cloud_uploader2 >> Edge(color="black", style="solid") >> gcp_storage