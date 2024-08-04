import streamlit as st

# Set the title of the app
st.title("Diplomat Distributors LTD Analytics Dashboard")

# Sidebar for navigation
st.sidebar.title("Navigation")
page = st.sidebar.selectbox("Select a page", ["Home", "Data Overview", "Visualizations", "Chat"])

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