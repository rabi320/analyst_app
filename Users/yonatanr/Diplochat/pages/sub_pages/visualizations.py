import streamlit as st
import pandas as pd
import pyodbc
import os
db_password = os.getenv('DB_PASSWORD') 

@st.cache_resource
def load_data():
    """
    Load data from SQL Server and return it as a DataFrame.
    """
    conn = pyodbc.connect(driver='{ODBC Driver 17 for SQL Server}',
                          server='diplomat-analytics-server.database.windows.net',
                          database='NBO-DB',
                          uid='analyticsadmin', 
                          pwd=db_password)

    query = 'SELECT * FROM [dbo].[DW_DIM_STORENEXT_BY_INDUSTRIES_ITEMS]'
    df = pd.read_sql_query(query, conn)
    conn.close()  # Close the connection after reading the data
    return df

def run():
       st.header("Data Visualizations")

       # Load data
       df = load_data()

       # Select a visualization type
       viz_type = st.selectbox("Choose a visualization type", ["Bar Chart by Category", "Bar Chart by Class"])

       if viz_type == "Bar Chart by Category":
           # Create a bar chart of counts by Category
           st.subheader("Bar Chart of Item Counts by Category")
           if 'Category_Name' in df.columns:
               industry_counts = df['Category_Name'].value_counts()
               
               chart_data = pd.DataFrame(industry_counts.values.reshape(1, -1),columns = industry_counts.index.tolist())

               st.bar_chart(chart_data)


       elif viz_type == "Bar Chart by Class":
           # Create a bar chart of counts by Class
           st.subheader("Bar Chart of Item Counts by Class")
           if 'Class_Name' in df.columns:
               class_counts = df['Category_Name'].value_counts()
               
               chart_data = pd.DataFrame(class_counts.values.reshape(1, -1),columns = class_counts.index.tolist())

               st.bar_chart(chart_data)


if __name__ == "__main__":
    run()