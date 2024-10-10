import streamlit as st
import requests
from http import HTTPStatus
import os

# Function to display the search engine page
def display_search_engine():
    st.title("Search Engine")

    # Fetch prompts from the backend
    response = requests.get("http://"+ os.getenv("HOSTNAME") + ":8000/listprompts/20")
    response_data = response.json()

    if response_data['status'] == HTTPStatus.OK:
        prompts_list = response_data['message']
        prompts_dict = {item['question']: item['task_id'] for item in prompts_list}
        prompts = list(prompts_dict.keys())

        selected_prompt = st.selectbox("Select a prompt:", prompts)

        if st.button("Load Data") and selected_prompt:
            selected_task_id = prompts_dict[selected_prompt]

            url = "http://"+ os.getenv("HOSTNAME") + ":8000"
            load_response = requests.get(f"{url}/loadprompt/{selected_task_id}")
            load_data = load_response.json()

            if load_data['status'] == HTTPStatus.OK:
                file_name = 'empty' if load_data['message']['file_name'] == '' else load_data['message']['file_name']
                st.text_area("Question", value=load_data['message']['question'], key="task_question", disabled=True, height=100)
                st.text_input("Level", value=load_data['message']['level'], key="task_level", disabled=True)
                st.text_input("File", value=file_name, key="task_filename", disabled=True)
            else:
                st.error("Failed to load prompt data.")

        if st.button("Generate Response") and selected_prompt:
            selected_task_id = prompts_dict[selected_prompt]
            st.session_state['task_id'] = selected_task_id
            st.session_state['page'] = 'validation'
            st.session_state['action'] = True

    else:
        st.error("Failed to fetch prompts from the server.")


# Main app function to display the required page
def main():
    if 'page' not in st.session_state:
        st.session_state['page'] = 'searchengine'

    # Handle navigation between pages
    if st.session_state['page'] == 'searchengine':
        display_search_engine()
    elif st.session_state['page'] == 'validation':
        display_validation_page()  

if __name__ == '__main__':
    main()