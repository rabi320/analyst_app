import streamlit as st  
import pyodbc  
import pandas as pd  
from datetime import datetime  

@st.cache_data(show_spinner="Loading data.. this can take a few minutes, feel free to grab a coffee ☕") 
def load_data():  
    conn = pyodbc.connect(driver='{ODBC Driver 17 for SQL Server}',  
                          server='diplomat-analytics-server.database.windows.net',  
                          database='NBO-DB',  
                          uid='analyticsadmin', pwd='Analytics12345')  
  

    #Define tables and queries
    tables = {
        'DW_FACT_STORENEXT_BY_INDUSTRIES_SALES': """
            SELECT Day, Barcode, Format_Name, Sales_NIS, Sales_Units, Price_Per_Unit
            FROM [dbo].[DW_FACT_STORENEXT_BY_INDUSTRIES_SALES]
            WHERE Day BETWEEN '2024-05-01' AND '2024-05-31'
        """,
        'DW_DIM_STORENEXT_BY_INDUSTRIES_ITEMS': """
            SELECT Barcode, Item_Name, Category_Name, Sub_Category_Name, Brand_Name, Sub_Brand_Name, Supplier_Name
            FROM [dbo].[DW_DIM_STORENEXT_BY_INDUSTRIES_ITEMS]
            WHERE Category_Name = N'חטיפים'
        """,
        # 'DW_CHP': """
        #     SELECT ITEM_DESCRIPION, BARCODE, CHAIN_CODE, STORE_CODE, CHAIN, STORE, ADDRESS, CITY, SELLOUT_DESCRIPTION, STORENEXT_CATEGORY, SUPPLIER, FILE_DATE, PRICE, SELLOUT_PRICE, SALE_ID
        #     FROM [dbo].[DW_CHP]
        #     WHERE STORENEXT_CATEGORY = N'חטיפים' AND FILE_DATE BETWEEN '2024-05-30' AND '2024-05-31'
        # """
    }

    dataframes = {}  
    for table, query in tables.items():  
        chunks = []  
        chunk_size = 10000  
        total_rows = pd.read_sql_query(f"SELECT COUNT(*) FROM ({query}) AS count_query", conn).iloc[0, 0]  
        total_chunks = (total_rows // chunk_size) + 1  
  
        for i, chunk in enumerate(pd.read_sql_query(query, conn, chunksize=chunk_size)):  
            chunks.append(chunk)  
          
        df = pd.concat(chunks, ignore_index=True)  
        dataframes[table] = df  
  
    conn.close()  
    return dataframes  



def run():  
    st.title("Dynamic Python Script Execution")  
  
    dataframes = load_data()  
    
    # Assigning dataframes to variables
    stnx_sales = dataframes['DW_FACT_STORENEXT_BY_INDUSTRIES_SALES']
    stnx_items = dataframes['DW_DIM_STORENEXT_BY_INDUSTRIES_ITEMS']
    # chp = dataframes['DW_CHP']

    
    script = st.text_area("Enter your Python code:", height=200,  
                          value='answer = "Hello, this is the answer!"')  
  
    if st.button("Run Code"):  
        local_context = {'stnx_sales':stnx_sales,'stnx_items':stnx_items,'pd':pd}  
  
        try:  
            exec(script.strip(), {}, local_context)  
            answer = local_context.get('answer', "No answer found.")  
            st.write(answer)
              
        except Exception as e:  
            st.error(f"Error: {e}")  
  
if __name__ == "__main__":  
    run()  
