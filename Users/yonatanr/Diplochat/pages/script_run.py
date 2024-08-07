import streamlit as st  
import pyodbc  
import pandas as pd 
from datetime import datetime


def run():
    st.title("Dynamic Python Script Execution")  
    with st.spinner("Loadin data.. this can take a few minutes, feel free to grab a cofee ☕"):
        # Connect to SQL Server
        conn = pyodbc.connect(driver='{ODBC Driver 17 for SQL Server}',
                                server='diplomat-analytics-server.database.windows.net',
                                database='NBO-DB',
                                uid='analyticsadmin', pwd='Analytics12345')


        # Define tables and queries
        # tables = {
        #     'DW_FACT_STORENEXT_BY_INDUSTRIES_SALES': """
        #         SELECT Day, Barcode, Format_Name, Sales_NIS, Sales_Units, Price_Per_Unit
        #         FROM [dbo].[DW_FACT_STORENEXT_BY_INDUSTRIES_SALES]
        #         WHERE Day BETWEEN '2024-03-01' AND '2024-05-31'
        #     """,
        #     'DW_DIM_STORENEXT_BY_INDUSTRIES_ITEMS': """
        #         SELECT Barcode, Item_Name, Category_Name, Sub_Category_Name, Brand_Name, Sub_Brand_Name, Supplier_Name
        #         FROM [dbo].[DW_DIM_STORENEXT_BY_INDUSTRIES_ITEMS]
        #         WHERE Category_Name = N'חטיפים'
        #     """,
        #     'DW_CHP': """
        #         SELECT ITEM_DESCRIPION, BARCODE, CHAIN_CODE, STORE_CODE, CHAIN, STORE, ADDRESS, CITY, SELLOUT_DESCRIPTION, STORENEXT_CATEGORY, SUPPLIER, FILE_DATE, PRICE, SELLOUT_PRICE, SALE_ID
        #         FROM [dbo].[DW_CHP]
        #         WHERE STORENEXT_CATEGORY = N'חטיפים' AND FILE_DATE BETWEEN '2024-03-01' AND '2024-05-31'
        #     """
        # }
        tables = {
      'DW_CHP': """
                SELECT ITEM_DESCRIPION, BARCODE, CHAIN_CODE, STORE_CODE, CHAIN, STORE, ADDRESS, CITY, SELLOUT_DESCRIPTION, STORENEXT_CATEGORY, SUPPLIER, FILE_DATE, PRICE, SELLOUT_PRICE, SALE_ID
                FROM [dbo].[DW_CHP]
                WHERE STORENEXT_CATEGORY = N'חטיפים' AND FILE_DATE BETWEEN '2024-05-01' AND '2024-05-31'
            """,

        }
 

        dataframes = {}  
        for table, query in tables.items():  
            # Fetch data in chunks  
            chunks = []  
            chunk_size = 10000  # Adjust based on available memory and performance considerations  
            total_rows = pd.read_sql_query(f"SELECT COUNT(*) FROM ({query}) AS count_query", conn).iloc[0, 0]  
            total_chunks = (total_rows // chunk_size) + 1  
            
            chunk_progress_bar = st.progress(0)  
            
            for i, chunk in enumerate(pd.read_sql_query(query, conn, chunksize=chunk_size)):  
                # Concatenate all chunks into a single DataFrame  
                chunks.append(chunk)  
                progress = (i + 1) / total_chunks  
                chunk_progress_bar.progress(progress)  
            
            df = pd.concat(chunks, ignore_index=True)  
            dataframes[table] = df  
    
        conn.close()  
    
    
        st.success("Data loading complete!")
        # Assigning dataframes to variables
        # stnx_sales = dataframes['DW_FACT_STORENEXT_BY_INDUSTRIES_SALES']
        # stnx_items = dataframes['DW_DIM_STORENEXT_BY_INDUSTRIES_ITEMS']
        chp = dataframes['DW_CHP']
      
        

    # Input area for the user to enter their code  
    script = st.text_area("Enter your Python code:", height=200,   
                           value='answer = "Hello, this is the answer!"')  # Default example code  
  
    if st.button("Run Code"):  
        # Create a local context for exec  
        local_context = {'chp': chp} 
  
        try:  
            # Execute the user's script  
            exec(script.strip(), {}, local_context)  # Pass local_context for variable storage  
              
            # Now 'answer' should be available in the local context  
            answer = local_context.get('answer', "No answer found.")  # Safely get answer  
              
            # Display the 'answer' variable  
            st.success(f"The answer is: {answer}")  
  
        except Exception as e:  
            # Display any error that occurs during execution  
            st.error(f"Error: {e}")  
  
# Call the run function to execute the script  
if __name__ == "__main__":  
    run()  
