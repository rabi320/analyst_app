import streamlit as st  
import streamlit_authenticator as stauth  
import yaml
from yaml.loader import SafeLoader
from pages.analyst_chat import run as analyst_chat_page

# from st_pages import hide_pages
  
with open('config.yaml') as file:
    config = yaml.load(file, Loader=SafeLoader)

authenticator = stauth.Authenticate(
    config['credentials'],
    config['cookie']['name'],
    config['cookie']['key'],
    config['cookie']['expiry_days'],
    config['pre-authorized']
)

st.title('Diplomat AI')

# Login widget  
authentication_status = authenticator.login()  


# hide_pages([ "Map"])

# Adjusted authentication status handling  
if st.session_state['authentication_status']:  
    st.sidebar.markdown("![](https://www.diplomat-global.com/wp-content/uploads/2018/06/logo.png)")
    authenticator.logout(location = 'sidebar')  # Add logout functionality  
    st.write(f'Welcome *{st.session_state["name"]}*')  # Display welcome message 

    analyst_chat_page() 

    
    


