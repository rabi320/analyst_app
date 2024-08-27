import streamlit as st  
import streamlit_authenticator as stauth  
import yaml
from yaml.loader import SafeLoader
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

st.title('Diplomat LTD Analytics')

# Login widget  
authentication_status = authenticator.login()  


# hide_pages([ "Map"])

# Adjusted authentication status handling  
if st.session_state['authentication_status']:  
    
    # reset password
    try:
        if authenticator.reset_password(st.session_state['username']):
            st.success('Password modified successfully')
            with open('../config.yaml', 'w') as file:
                yaml.dump(config, file, default_flow_style=False)
    except Exception as e:
        st.error(e)

    authenticator.logout()  # Add logout functionality  
    st.write(f'Welcome *{st.session_state["name"]}*')  # Display welcome message  
    st.title('Diplomat AI')  # Replace with your actual content  
  
    # Sidebar for navigation  
    st.sidebar.title("Navigation")  
    # page = st.sidebar.selectbox("Select a page", ["Home", "Data Overview", "Visualizations", "Chat", 'Analyst Chat', "Map", 'Inner Code'])
    # page = st.sidebar.selectbox("Select a page", ["Home", "Data Overview", "Visualizations", "Chat", 'Analyst Chat', "Map"])
    page = st.sidebar.selectbox("Select a page", ["Home", 'Analyst Chat',"Chat"])      
  
    # Load the corresponding page  
    if page == "Home":  
        from pages.home import run as home_page  
        home_page()  
    # elif page == "Data Overview":  
    #     from pages.data_overview import run as data_overview_page  
    #     data_overview_page()  
    # elif page == "Visualizations":  
    #     from pages.visualizations import run as visualizations_page  
    #     visualizations_page()  
    elif page == "Chat":  
        from pages.chat import run as chat_page  
        chat_page()  
    elif page == "Analyst Chat":  
        from pages.analyst_chat import run as analyst_chat_page  
        analyst_chat_page()  
    # elif page == "Map":  
        # from pages.map import run as display_map  
        # display_map()  
    # elif page == "Inner Code":  
        # from pages.script_run import run as script_run  
        # script_run()  

    # Sidebar navigation
    # st.sidebar.page_link('show_data.py', label='Home')
    # st.sidebar.page_link('pages/analyst_chat.py', label='Diplochat')



elif st.session_state['authentication_status'] is False:  
    st.error('Username/password is incorrect')  
elif st.session_state['authentication_status'] is None:  
    st.warning('Please enter your username and password')  