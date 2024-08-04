import streamlit as st
import pandas as pd
import pyodbc
import matplotlib.pyplot as plt
import seaborn as sns

@st.cache_resource
def load_data():
    conn = pyodbc.connect(driver='{ODBC Driver 17 for SQL Server}',
                          server='diplomat-analytics-server.database.windows.net',
                          database='NBO-DB',
                          uid='analyticsadmin', pwd='Analytics12345')

    query = 'SELECT * FROM [dbo].[DW_DIM_STORENEXT_BY_INDUSTRIES_ITEMS]'
    df = pd.read_sql_query(query, conn)
    return df

def run():
    st.header("Data Visualizations")

    # Load data
    df = load_data()

    # Select a visualization type
    viz_type = st.selectbox("Choose a visualization type", ["Category", "Class"])

    if viz_type == "Bar Chart":
        # Create a bar chart
        st.subheader("Bar Chart of Item Counts by Industry")
        industry_counts = df['Category_Name'].value_counts()  # Replace with actual column name
        fig, ax = plt.subplots()
        sns.barplot(x=industry_counts.index, y=industry_counts.values, ax=ax)
        ax.set_title("Count of Items by Category")
        plt.xticks(rotation=45)
        st.pyplot(fig)

    elif viz_type == "Line Chart":
        # Create a bar chart
        st.subheader("Bar Chart of Item Counts by Class")
        industry_counts = df['Class_Name'].value_counts()  # Replace with actual column name
        fig, ax = plt.subplots()
        sns.barplot(x=industry_counts.index, y=industry_counts.values, ax=ax)
        ax.set_title("Count of Items by Class")
        plt.xticks(rotation=45)
        st.pyplot(fig)