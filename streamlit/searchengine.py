import streamlit as st
import requests
from http import HTTPStatus
import os
import json
from overview import display_overview_page
from validation import display_validation_page

# Function to display the search engine page
def display_search_engine():
    st.title("Search Engine")

     # Back button to return to home (login) page
    if st.button("Logout"):
        if 'token' in st.session_state:
            del st.session_state['token']
        st.session_state['logged_in'] = False
        st.session_state['page'] = 'overview'
        st.success("Logged out successfully!")
        display_overview_page()

    dataset_type = st.selectbox(
        "Select Dataset Type", 
        options=["validation", "test"], 
        index=0,  # Default to "validation"
    )

    auth_token = st.session_state['token']

    if st.button("Fetch Prompts"):
        # Define your headers here (authorization or any other required headers)
        headers = {
            "Authorization": f"Bearer {auth_token}",
            "Content-Type": "application/json"
        }


        # Fetch prompts from the backend
        response = requests.get(f"http://{os.getenv('HOSTNAME')}:8000/listprompts?count=20&type={dataset_type}", headers=headers)
        response_data = response.json()

        if response_data['status'] == HTTPStatus.OK:
            prompts_list = response_data['message']
            prompts_dict = {item['question']: item['task_id'] for item in prompts_list}
            prompts = list(prompts_dict.keys())

            st.session_state['prompts'] = prompts
            st.session_state['prompts_dict'] = prompts_dict

        else:
            st.error("Failed to fetch prompts from the server.")

    # Check if prompts are available in session_state
    if 'prompts' in st.session_state:
        prompts = st.session_state['prompts']
        prompts_dict = st.session_state['prompts_dict']

        # Display the fetched prompts in a selectbox
        selected_prompt = st.selectbox("Select a prompt:", prompts)

        # Store the selected prompt in session_state
        if selected_prompt:
            st.session_state['selected_prompt'] = selected_prompt
            st.session_state['selected_task_id'] = prompts_dict[selected_prompt]
    

    # Check if a prompt has been selected
    if 'selected_prompt' in st.session_state:
        selected_task_id = st.session_state['selected_task_id']

        # Load Data button after prompt selection
        if st.button("Load Data"):
            url = f"http://{os.getenv('HOSTNAME')}:8000"
            headers = {
                "Authorization": f"Bearer {auth_token}",
                "Content-Type": "application/json"
            }
            # Fetch the data for the selected prompt using the task ID
            load_response = requests.get(f"{url}/loadprompt/{selected_task_id}", headers=headers)
            load_data = load_response.json()

            if load_data['status'] == HTTPStatus.OK:
                file_name = 'empty' if load_data['message']['file_name'] == '' else load_data['message']['file_name']
                st.text_area("Question", value=load_data['message']['question'], key="task_question", disabled=True, height=100)
                st.text_input("Level", value=load_data['message']['level'], key="task_level", disabled=True)
                st.text_input("File", value=file_name, key="task_filename", disabled=True)
            else:
                st.error("Failed to load prompt data.")

        # Select a PDF extractor tool
        extractor_tool = st.selectbox(
            "Select a PDF extractor tool:",
            options=["PyMuPdf", "Adobe Extract API", "Azure AI Document Intelligence"]
        )

        # Fetch the extracted data using the selected tool
        pdf_extracted_data = None
        if extractor_tool == "PyMuPdf":
            pdf_extracted_data = "Extracted data from PyMuPdf"
        elif extractor_tool == "Adobe":
            pdf_extracted_data = "Extracted data from Adobe"
        elif extractor_tool == "Azure":
            pdf_extracted_data = "Extracted data from Azure"


        # Generate Response button
        if st.button("Generate Response"):
            # Prepare the data to send to the server
            service_mapping = {
                "PyMuPdf": "pymupdf",
                "Adobe Extract API": "adobe",
                "Azure AI Document Intelligence": "azure"
            }
            service = service_mapping.get(extractor_tool)
            st.session_state['service'] = service

            # payload = {
            #     "task_id": selected_task_id,
            #     "service": service
            # }

            st.session_state['page'] = 'validation'
            st.session_state['action'] = True