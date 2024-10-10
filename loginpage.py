# login.py
import streamlit as st
import requests
from http import HTTPStatus
import os

def login(email, password):
    data = { 
        'email': email, 
        'password': password
    }
    response = requests.post('http://'+ os.getenv("HOSTNAME") +':8000/login', json=data)
    return response.json()

def register(first_name, last_name, email, phone, password):
    data = { 
        'first_name': first_name,
        'last_name': last_name, 
        'phone': phone,
        'email': email, 
        'password': password
    }
    response = requests.post('http://'+ os.getenv("HOSTNAME") +':8000/register', json=data)
    return response.json()

def display_login_page():
    st.title("Welcome to GPT Search Engine")
    st.subheader("Login / Register Page")

    # Option to choose between Login or Register
    option = st.selectbox("Select an action:", ["Login", "Register"], key="action_select")

    # Initialize session state variables if not already set
    if 'email' not in st.session_state:
        st.session_state['email'] = ""
    if 'password' not in st.session_state:
        st.session_state['password'] = ""

    if option == "Login":
        email = st.text_input("Email", value="", key="login_email")
        password = st.text_input("Password", type="password", value="", key="login_password")

        if st.button("Login"):
            response = login(email, password)
            if response['status'] == HTTPStatus.OK:
                st.success("Logged in successfully!")
                st.session_state['logged_in'] = True  
                st.session_state['user_id'] = response['user_id']
                # Navigate to the search engine
                st.session_state['page'] = 'searchengine'  
            else:
                st.error("Login failed. Please try again.")

    elif option == "Register":
        first_name = st.text_input("First Name", key="register_first_name")
        last_name = st.text_input("Last Name", key="register_last_name")
        email = st.text_input("Email", key="register_email")
        phone = st.text_input("Phone", key="register_phone")
        password = st.text_input("Password", type="password", key="register_password")
        
        if st.button("Register"):
            response = register(first_name, last_name, email, phone, password)
            if response['status'] == HTTPStatus.OK:
                st.success("Registered successfully!")
                st.session_state['logged_in'] = True  
                st.session_state['user_id'] = response['user_id']
                # Navigate to the search engine
                st.session_state['page'] = 'searchengine'  
            else:
                st.error("Registration failed. Please try again.")