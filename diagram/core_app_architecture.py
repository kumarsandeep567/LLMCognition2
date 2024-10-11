from diagrams import Diagram, Edge, Cluster
from diagrams.gcp.storage import Storage
from diagrams.aws.database import RDS
from diagrams.custom import Custom
from diagrams.programming.framework import FastAPI
from diagrams.onprem.client import Users, Client

with Diagram("Core Application Service", show=False, direction="LR"):

    with Cluster("Docker Container"):
        fastapi = FastAPI("FastAPI application\n(Backend)")
    
    with Cluster("Docker Container"):
        streamlit = Custom("Streamlit application\n(Frontend)", "./images/Streamlit.png")
    
    # Define nodes
    sql = RDS("AWS RDS\n(Text, JSON,\nUser data, Analytics)")
    storage = Storage("Google Cloud Storage Bucket\n(CSV, Image, Audio, Video)")
    users = Users("End Users")
    openai = Custom("OpenAI GPT-4o / OpenAI Whisper", "./images/OpenAI.png")
    
    # Define connections
    sql << Edge(color="black", style="solid") >> fastapi
    storage >> Edge(color="black", style="solid") >> fastapi
    
    fastapi << Edge(color="black", style="solid", label="REST APIs") >> streamlit
    fastapi << Edge(color="black", style="solid") >> openai

    streamlit << Edge(color="black", style="solid") >> users