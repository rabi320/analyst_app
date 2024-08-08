import streamlit as st

# Set the title of the app
st.title("Diplomat Distributors LTD Analytics Dashboard")

# Sidebar for navigation
st.sidebar.title("Navigation")
page = st.sidebar.selectbox("Select a page", ["Home", "Data Overview", "Visualizations", "Chat","Map",'Inner Code'])

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
    # Import the map page
    from pages.map import run as display_map
    display_map()

elif page == "Inner Code":
    # Import the map page
    from pages.script_run import run as script_run
    script_run()
