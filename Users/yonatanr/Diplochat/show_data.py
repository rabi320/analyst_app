import streamlit as st  
import streamlit_authenticator as stauth  
import yaml  
  
# Load the configuration from the YAML file  
with open('config.yaml') as file:  
    config = yaml.safe_load(file)  
  
# Retrieve cookie name and signature key from the config  
cookie_name = config['auth']['cookie_name']  
signature_key = config['auth']['signature_key']  
  
# Define your user credentials here  
usernames = ['yonatanr@diplomat-global.com', 'avit@diplomat-global.com']  # Add your usernames  
passwords = ['Yonirabi8!', 'avit12345678!']  # Add your corresponding passwords  
names = ['Yonatan Rabinovich', 'Avi Tuval']  # Add user display names  
  
# Create an authenticator object using values from the YAML file  
authenticator = stauth.Authenticate(  
    usernames=usernames,  
    names=names,  
    passwords=passwords,  
    cookie_name=cookie_name,  
    signature_key=signature_key,  
    cookie_expiry_days=30  
)  
  
# Login widget  
name, authentication_status = authenticator.login('Login', 'main')  
  
if authentication_status:  
    st.title("Diplomat Distributors LTD Analytics Dashboard")  
      
    # Sidebar for navigation  
    st.sidebar.title("Navigation")  
    page = st.sidebar.selectbox("Select a page", ["Home", "Data Overview", "Visualizations", "Chat", 'Analyst Chat', "Map", 'Inner Code'])  
  
    # Load the corresponding page  
    if page == "Home":  
        from pages.home import run as home_page  
        home_page()  
    elif page == "Data Overview":  
        from pages.data_overview import run as data_overview_page  
        data_overview_page()  
    elif page == "Visualizations":  
        from pages.visualizations import run as visualizations_page  
        visualizations_page()  
    elif page == "Chat":  
        from pages.chat import run as chat_page  
        chat_page()  
    elif page == "Analyst Chat":  
        from pages.analyst_chat import run as analyst_chat_page  
        analyst_chat_page()  
    elif page == "Map":  
        from pages.map import run as display_map  
        display_map()  
    elif page == "Inner Code":  
        from pages.script_run import run as script_run  
        script_run()  
  
elif authentication_status is False:  
    st.error('Username/password is incorrect')  
  
elif authentication_status is None:  
    st.warning('Please enter your username and password')  