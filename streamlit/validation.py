import streamlit as st
import requests
from http import HTTPStatus
import os
from overview import display_overview_page

# Function to query GPT model response
def query_gpt(task_id, updated_steps=None):
    data = { 
        'task_id': task_id,
        'service': st.session_state['service'],
        'updated_steps': updated_steps
    }
    
    if updated_steps:
        data['updated_steps'] = updated_steps

    auth_token = st.session_state['token']
    headers = {
            "Authorization": f"Bearer {auth_token}",
            "Content-Type": "application/json"
        }
    
    response = requests.post('http://'+ os.getenv("HOSTNAME") +':8000/querygpt', json=data, headers=headers)

    if response.status_code == HTTPStatus.OK:
        return response.json()
    else:
        return {'status': response.status_code, 'message': 'Error fetching GPT response.'}

# Function to display the validation page and allow editing of annotator steps
def display_validation_page():
    st.title("Response Validation")
    task_id = st.session_state['selected_task_id']

    if st.button("Logout", key="logout_button"):
        if 'token' in st.session_state:
            del st.session_state['token']
        st.session_state['logged_in'] = False
        st.session_state['page'] = 'overview'
        st.success("Logged out successfully!")
        display_overview_page()

    if st.button("Back", key="back_button"):
        st.session_state['page'] = 'searchengine'

    # Initialize the final_answer and gpt_response placeholders
    final_answer = "This is a sample GPT answer."
    gpt_response = "This is a sample GPT response."
    annotation_steps = "This is a sample annotation step."

    if 'final_answer' in st.session_state:
        final_answer = st.session_state['final_answer']
    
    if 'gpt_response' in st.session_state:
        gpt_response = st.session_state['gpt_response']
    
    if 'annotation_steps' in st.session_state:
        annotation_steps = st.session_state['annotation_steps']

    if 'task_id' in st.session_state:
        task_id = st.session_state['selected_task_id']

    # Check if task_id is available in session state
    if 'action' in st.session_state:
        if st.session_state['action'] == True:
            # Get GPT response from the FastAPI server
            response = query_gpt(task_id)
                
            if response['status'] == HTTPStatus.OK:
                st.write(response)
                final_answer = response.get('final_answer', final_answer)
                st.session_state['final_answer'] = final_answer

                gpt_response = response.get('gpt_response', gpt_response)
                st.session_state['gpt_response'] = gpt_response

                annotation_steps = response.get('annotation_steps', annotation_steps)
                st.session_state['annotation_steps'] = annotation_steps
                    
                st.session_state['action'] = False

    # Initialize the count in session state if it doesn't exist
    if 'count' not in st.session_state:
        st.session_state['count'] = 0

    # Create a row with two columns: one for the title and another for the back button
    col1, col2 = st.columns([5, 1]) 

    col1, col2 = st.columns(2)

    with col1:
        gpt_response_box = st.text_area("GPT Response", value=gpt_response, height=200)

    with col2:
        validation_response_box = st.text_area("Validation Response", value=final_answer, height=200)

    # Create two buttons: Compare and Mark as Correct
    button_col1, button_col2 = st.columns([2, 1]) 

    with button_col1:
        # Compare the responses button
        if st.button("Compare Responses", key="compare_button"):
            if gpt_response_box.strip() == validation_response_box.strip():
                st.success("The GPT response and validation response match!")
            else:
                st.error("The GPT response and validation response do NOT match!")

    with button_col2:
        # Mark the response as correct button
        if st.button("Mark as Correct"):
            data = { 
                'task_id': task_id
            }
            auth_token = st.session_state['token']
            headers = {
                    "Authorization": f"Bearer {auth_token}",
                    "Content-Type": "application/json"
                }
            
            response = requests.post('http://'+ os.getenv("HOSTNAME") +':8000/markcorrect', headers=headers, json=data)
            st.success("The GPT response and validation response match!")

    # Editable annotator metadata box
    with st.expander("Edit Annotator Metadata"):
        updated_annotation_steps = st.text_area("Steps", value=annotation_steps, height=300)

    # Button to send updated annotator steps, task_id, and user_id to FastAPI and regenerate GPT response
    if st.button("Update and Regenerate GPT Response"):
        new_response = query_gpt(task_id=task_id, updated_steps=updated_annotation_steps)
    
        if new_response['status'] == HTTPStatus.OK:
            st.session_state['gpt_response'] = new_response['gpt_response']
            st.success("GPT response regenerated!")
            st.success(updated_annotation_steps)
            st.session_state['count'] += 1  # Increment the count
            st.session_state['page'] = 'validation'

            # Instead of st.experimental_rerun(), using JavaScript to reload the page
            st.write('<script>location.reload()</script>', unsafe_allow_html=True)
        else:
            st.error("Failed to regenerate GPT response.")

        st.session_state['action'] = False

    # Feedback Section
    st.subheader("Feedback")
    feedback_text = st.text_area("Please provide your feedback here:", height=100)

    if st.button("Submit", key="submit_feedback_button"):
        if feedback_text.strip():
            # Here, you would typically save the feedback to a database or handle it accordingly
            st.success("Thank you for your feedback!")
            # Optionally clear the feedback box after submission
            st.session_state['feedback'] = feedback_text
            feedback_text = ""
        else:
            st.error("Please enter your feedback before submitting.")

def main():
    display_validation_page()

# For testing purposes, you can initialize a session state like this:
if __name__ == '__main__':
    if 'task_id' not in st.session_state:
        st.session_state['task_id'] = 'example_task_id'
    if 'user_id' not in st.session_state:
        st.session_state['user_id'] = 'example_user_id'
    
    main()
