import streamlit as st
from loginpage import display_login_page
from analytics import display_analytics_page
from searchengine import display_search_engine
from validations import display_validation_page

def main():
    # Initialize session state for page navigation
    if 'page' not in st.session_state:
        st.session_state['page'] = 'login'  # Set login as the default page

    # Display the appropriate page based on session state
    if st.session_state['page'] == 'login':
        display_login_page()
    elif st.session_state['page'] == 'analytics':
        display_analytics_page()
    elif st.session_state['page'] == 'searchengine':
        display_search_engine()
    elif st.session_state['page'] == 'validation':
        display_validation_page()

def display_navigation():
    """Display navigation options."""
    st.sidebar.title("Navigation")
    selected_page = st.sidebar.radio("Select Page:", ["Search Engine", "App Analytics"])

    if selected_page == "Search Engine":
        st.session_state['page'] = 'searchengine'
    elif selected_page == "App Analytics":
        st.session_state['page'] = 'analytics'

# Call display_navigation() only for relevant pages
if 'page' in st.session_state and st.session_state['page'] in ['searchengine', 'analytics']:
    display_navigation()

if __name__ == '__main__':
    main()