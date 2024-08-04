import streamlit as st  
import pandas as pd  
import pyodbc  
  
# Fetch data  


# Upload to SQL Server
conn = pyodbc.connect(driver='{ODBC Driver 17 for SQL Server}',
       server='diplomat-analytics-server.database.windows.net',
       database='NBO-DB',
       uid='analyticsadmin', pwd='Analytics12345')


query = 'SELECT * FROM [dbo].[DW_DIM_STORENEXT_BY_INDUSTRIES_ITEMS]'


# Execute the query and read the results into a DataFrame  
df = pd.read_sql_query(query, conn)  

conn.close()

df.head()



st.title("SQL Data Viewer")  
    
    
st.subheader("Table Head")  
st.write(df.head(10))  
  

