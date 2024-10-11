import streamlit as st
from loginpage import display_login_page
from overview import display_overview_page
from searchengine import display_search_engine
from validation import display_validation_page

def main():
    # Initialize session state for page navigation
    if 'page' not in st.session_state:
        st.session_state['page'] = 'overview'  # Set login as the default page

    # Display the appropriate page based on session state
    if st.session_state['page'] == 'overview':
        display_overview_page()
    elif st.session_state['page'] == 'searchengine':
        display_search_engine()
    elif st.session_state['page'] == 'validation':
        display_validation_page()


if __name__ == '__main__':
    main()
