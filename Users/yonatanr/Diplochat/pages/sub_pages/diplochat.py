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

st.title('Diplomat AI')

# Clear the cache on app start or refresh
if 'cache_cleared' not in st.session_state:
    st.cache_data.clear()
    st.session_state.cache_cleared = True


# Login widget  
authentication_status = authenticator.login()  


# hide_pages([ "Map"])

# Adjusted authentication status handling  
if st.session_state['authentication_status']:  
    
    st.sidebar.markdown("![](https://www.diplomat-global.com/wp-content/uploads/2018/06/logo.png)")
    authenticator.logout(location = 'sidebar')  # Add logout functionality  
    st.write(f'Welcome *{st.session_state["name"]}*')  # Display welcome message  
  
    # Sidebar for navigation  
    st.sidebar.title("Navigation")
    # page = st.sidebar.selectbox("Select a page", ["Home", 'Analyst Chat',"Chat"])
    page = st.sidebar.selectbox("Select a page", ["Home", 'Analyst Chat',"Chat"])
    

    # page = st.sidebar.selectbox("Select a page", ["Home", "Data Overview", "Visualizations", "Chat", 'Analyst Chat', "Map", 'Inner Code'])
    # page = st.sidebar.selectbox("Select a page", ["Home", "Data Overview", "Visualizations", "Chat", 'Analyst Chat', "Map"])
          
        
    # st.sidebar.header("Data Selection")
    # resulotion_type = st.sidebar.selectbox("Choose resulotion:", ["Weekly", "Monthly"])

    # Choose resolution type
    # Initialize resolution type in session state if not already set
    # Initialize resolution type in session state if not already set
    if 'resolution_type' not in st.session_state:
        st.session_state.resolution_type = "weekly"  # default value

    # Sidebar radio button for choosing resolution type
    selected_resolution = st.sidebar.radio("Choose resolution:", ["weekly", "monthly"], index=0 if st.session_state.resolution_type == "weekly" else 1)

    # Check if the resolution type has changed and rerun/cache if it has
    if selected_resolution != st.session_state.resolution_type:
        st.session_state.resolution_type = selected_resolution
        # Use a flag to clear the cache
        # st.session_state.cache_invalidated = True
        # st.rerun()  # This will rerun the whole app

    # Clear cached data if the cache invalidation flag is set
    # if 'cache_invalidated' in st.session_state:
    #     del st.session_state.cache_invalidated  # Resetting flag
        

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