import streamlit as st
import pandas as pd
import pyodbc
import matplotlib.pyplot as plt
import seaborn as sns

@st.cache_resource
def load_data():
    """
    Load data from SQL Server and return it as a DataFrame.
    """
    conn = pyodbc.connect(driver='{ODBC Driver 17 for SQL Server}',
                          server='diplomat-analytics-server.database.windows.net',
                          database='NBO-DB',
                          uid='analyticsadmin', 
                          pwd='Analytics12345')

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
            fig, ax = plt.subplots()
            sns.barplot(x=industry_counts.index, y=industry_counts.values, ax=ax)
            ax.set_title("Count of Items by Category")
            plt.xticks(rotation=45)
            st.pyplot(fig)
        else:
            st.error("Column 'Category_Name' does not exist in the DataFrame.")

    elif viz_type == "Bar Chart by Class":
        # Create a bar chart of counts by Class
        st.subheader("Bar Chart of Item Counts by Class")
        if 'Class_Name' in df.columns:
            class_counts = df['Class_Name'].value_counts()
            fig, ax = plt.subplots()
            sns.barplot(x=class_counts.index, y=class_counts.values, ax=ax)
            ax.set_title("Count of Items by Class")
            plt.xticks(rotation=45)
            st.pyplot(fig)
        st.error("Column 'Class_Name' does not exist in the DataFrame.")


if __name__ == "__main__":
    run()