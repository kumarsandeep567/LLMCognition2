import streamlit as st

def display_home_page():
    
    st.write("""
    # Welcome to the GAIA-GPT Evaluation Platform

    Experience a powerful, interactive platform designed to rigorously assess OpenAI's GPT models using HuggingFace's GAIA (General AI Assistant) benchmark dataset. Our Streamlit-based application streamlines the process of evaluating GPT's performance by automatically extracting content from PDF files, processing contextual information, and comparing generated responses against annotated solution steps.

    ## 🎯 How It Works

    #### New Users - Register
    - Click the "Register" button in the navigation bar
    - Fill in your details:
    - Email address
    - Password (create a secure password)
    - Personal information as required
    - Submit registration form
    - Look for a success message confirming your registration
    -Upon successful login, you'll be directed to the main dashboard         

    #### Existing Users - Login
    - Click the "Login" button
    - Enter your registered email
    - Enter your password
    - Click "Login" to access the platform
    - Upon successful login, you'll be directed to the main dashboard

    ### Select Your Prompt
    - Browse through curated prompts containing pdfs from the GAIA dataset
    - View question difficulty levels and associated PDF files
    - Choose prompts that match your evaluation needs

    ### Choose Your Extraction Tool
    - **PyMuPDF**: Ideal for straightforward text extraction with fast processing
    - **Adobe PDF Extract API**: Perfect for maintaining complex document layouts and formatting
    - **Azure AI Document Intelligence**: Advanced AI-powered extraction with superior accuracy

    ### Automated Processing
    - Fast API automatically extracts text from the PDF using your chosen method
    - Extracted content is formatted and optimized for GPT processing
    - Content is paired with your selected prompt for evaluation

    ### Response Generation & Comparison
    - GPT generates responses based on the extracted content and prompt
    - Fast API compares GPT generated answers with GAIA's validated solutions
    - View detailed validation results
    """)
