import streamlit as st  
import pandas as pd  
import pyodbc  
  
# Define the function to fetch data from the SQL server  
def fetch_data():  
    conn = pyodbc.connect(driver='{ODBC Driver 17 for SQL Server}',  
                          server='diplomat-analytics-server.database.windows.net',  
                          database='NBO-DB',  
                          uid='analyticsadmin',  
                          pwd='Analytics12345')  
    query = 'SELECT * FROM [dbo].[DW_DIM_STORENEXT_BY_INDUSTRIES_ITEMS]'  
    df = pd.read_sql_query(query, conn)  
    conn.close()  
    return df  
  
# Streamlit app  
def main():  
    st.title("SQL Data Viewer")  
      
    # Fetch data  
    df = fetch_data()  
      
    st.subheader("Table Head")  
    st.write(df.head())  
  
if __name__ == "__main__":  
    main()  
