import streamlit as st  
import pandas as pd  
import pyodbc  

@st.cache_data
def load_data():
    # Connect to SQL Server
    conn = pyodbc.connect(driver='{ODBC Driver 17 for SQL Server}',
                          server='diplomat-analytics-server.database.windows.net',
                          database='NBO-DB',
                          uid='analyticsadmin', pwd='Analytics12345')

    query = 'SELECT * FROM [dbo].[DW_DIM_STORENEXT_BY_INDUSTRIES_ITEMS]'

    # Execute the query and read the results into a DataFrame  
    df = pd.read_sql_query(query, conn)  
    conn.close()

    return df

def run():
    st.header("Data Overview")

    # Load data
    df = load_data()

    # Display the title and the head of the DataFrame
    st.subheader("Table Head")
    st.write(df.head(10))

    # Optionally, you can add more features to display more information about the DataFrame
    st.subheader("Total Records")
    st.write(f"Total records in the dataset: {df.shape[0]}")

    st.subheader("DataFrame Columns")
    st.write(df.columns.tolist())