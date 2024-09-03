import streamlit as st  
import pyodbc  
import pandas as pd  
from datetime import datetime  
from statsmodels.tsa.statespace.sarimax import SARIMAX
from openai import AzureOpenAI 
import warnings 
import time 
import re
from datetime import datetime
import pytz
from streamlit_feedback import streamlit_feedback
import os

# Suppress all warnings  
warnings.filterwarnings('ignore')   

# Sidebar navigation
# st.sidebar.page_link('show_data.py', label='Home')
# st.sidebar.page_link('pages/analyst_chat.py', label='Diplochat')


sys_msg = """  
You are an AI Data Analyst assistant for DIPLOMAT DISTRIBUTORS (1968) LTD, and you are coding in Python. 
The following datasets are already loaded in your Python IDE:  
  
1. **DW_FACT_STORENEXT_BY_INDUSTRIES_SALES** (`stnx_sales`)  
   - **Description**: This dataset provides daily sales figures by item across different market segments.  
   - **Columns**:  
     - `Day`: Date (datetime).  
     - `Barcode`: Item identifier.  
     - `Format_Name`: Market segmentation.  
     - `Sales_NIS`: Sales amount in NIS.  
     - `Sales_Units`: Quantity sold.  
     - `Price_Per_Unit`: Daily price per unit.  
   - **Note**: Filter the data for the date range between 2023-12-31 and 2024-9-1.  
  
2. **DW_DIM_STORENEXT_BY_INDUSTRIES_ITEMS** (`stnx_items`)  
   - **Description**: This is a dimension table containing attributes of items.  
   - **Columns**:  
     - `Barcode`: Item identifier.  
     - `Item_Name`: Name of the item.  
     - `Category_Name`: Name of the category.  
     - `Sub_Category_Name`: Name of the subcategory.  
     - `Brand_Name`: Name of the brand.  
     - `Sub_Brand_Name`: Name of the sub-brand.  
     - `Supplier_Name`: Name of the supplier.     
  
3. **DW_CHP_AGGR** (`chp`)  
   - **Description**: This fact table records daily snack prices by barcode and chain, including promotions.  
   - **Columns**:
     - `DATE`: Date (datetime).    
     - `BARCODE`: Item identifier.  
     - `CHAIN`: Name of the supermarket chain.
     - `AVG_PRICE`: Base price accross the stores in the chain.
     - `AVG_SELLOUT_PRICE`: Promotional price accross the stores in the chain, if null or missing then the no promotion is currently applies, address the current price.    
     - `NUMBER_OF_STORES`: Number of stores in the chain that reported containing this barcode.  
     - `SELLOUT_DESCRIPTION`: Hebrew description of sales promotions, if null or missing then the no promotion is currently applies.  
   - **Note**: to check barcodes attributes, connect this table with stnx_items 'Barcode' and get the relevant info, data here is from '2023-12-31' to '2024-09-01'.  

this is the code that already loaded the data to the IDE:

```python
db_password = os.getenv('DB_PASSWORD')
def load_data():  
    conn = pyodbc.connect(driver='{ODBC Driver 17 for SQL Server}',  
                          server='diplomat-analytics-server.database.windows.net',  
                          database='NBO-DB',  
                          uid='analyticsadmin', pwd=db_password)  
  

    #Define tables and queries
    tables = {
        'DW_FACT_STORENEXT_BY_INDUSTRIES_SALES': \"\"\"
            SELECT Day, Barcode, Format_Name, Sales_NIS, Sales_Units, Price_Per_Unit
            FROM [dbo].[DW_FACT_STORENEXT_BY_INDUSTRIES_SALES]
            WHERE Day BETWEEN '2023-12-31' AND '2024-09-01'
        \"\"\",
        'DW_DIM_STORENEXT_BY_INDUSTRIES_ITEMS': \"\"\"
            SELECT Barcode, Item_Name, Category_Name, Sub_Category_Name, Brand_Name, Sub_Brand_Name, Supplier_Name
            FROM [dbo].[DW_DIM_STORENEXT_BY_INDUSTRIES_ITEMS]
            
        \"\"\",
        'DW_CHP_AGGR': \"\"\"
            SELECT DATE,BARCODE,CHAIN,AVG_PRICE,AVG_SELLOUT_PRICE,SELLOUT_DESCRIPTION,NUMBER_OF_STORES
            FROM [dbo].[DW_CHP_AGGR]
            WHERE DATE BETWEEN '2023-12-31' AND '2024-09-01'
        \"\"\"
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

dataframes = load_data()


# Assigning dataframes to variables
stnx_sales = dataframes['DW_FACT_STORENEXT_BY_INDUSTRIES_SALES']
stnx_items = dataframes['DW_DIM_STORENEXT_BY_INDUSTRIES_ITEMS']
chp = dataframes['DW_CHP_AGGR']

# Convert date columns to datetime
stnx_sales['Day'] = pd.to_datetime(stnx_sales['Day'])
chp['DATE'] = pd.to_datetime(chp['DATE'])

```

Quesstions Convention - 

For any question you provide a code in python and in the end give the the answer in a python text variable named 'answer' after making the needed analysis.

* A very important note on Predictive analytics! - when asked about a future event, make a forecast based on forcasting methods such as SARIMA to get the desired prediction (make sure to deactivate any printed output from the chosen model).

Context for the Questions of stakeholders:

>Market Cap/Share (נתח שוק) - The Percent of total sales in NIS of a certain brand in his category by default or any other field if specifically requested by the user - 
meaning if asked about a certain brand's market cap, then you need sum that brand's sales in the chosen time frame (Can be monthly, weekly or daily, group your dataframe accordingly), and devide it by the total sales in that brand's category,
you need to merge the stnx_sales and stnx_items dataframes to obtain all the neccesary data for that.

>textual data - all the textual data here is hebrew so take that in mind while filtering dataframes.

>Competitors (מתחרים) - When requeting data about competitors, we are the supplier name 'דיפלומט' in the data and other supliers in the same category/ requested user's field are the competition. 

>Promotion Sales (מבצעים) - It is an actual promotion only where the 'AVG_SELLOUT_PRICE' a non-negative float number value. 
Final reminder: ensure that the 'answer' variable resembles a genuine prompt produced by a language model in the language used to address you!
"""  
sys_error = """  
You are an assistant that informs the user when their input is unclear,  
and you ask them to provide more details or rephrase their message in the same language they used.  
"""  


examples = [{'role': 'user', 'content': 'מהם ניתחי השוק של מותג פרינגלס בפילוח שבועי'},
 {'role': 'assistant',
  'content': '```python\n# Merging sales data with items data\nmerged_data = stnx_sales.merge(stnx_items, on=\'Barcode\', how=\'inner\')\n\n# Filtering for Pringles brand\npringles_data = merged_data[merged_data[\'Brand_Name\'] == \'פרינגלס\']\n\n# Grouping by week and calculating total sales in NIS for Pringles\npringles_weekly_sales = pringles_data.resample(\'W-Mon\', on=\'Day\').agg({\'Sales_NIS\': \'sum\'}).reset_index()\n\n# Calculating total sales for the snacks category\ntotal_weekly_sales = merged_data.resample(\'W-Mon\', on=\'Day\').agg({\'Sales_NIS\': \'sum\'}).reset_index()\n\n# Merging to calculate market cap\nmarket_cap_data = pringles_weekly_sales.merge(total_weekly_sales, on=\'Day\', suffixes=(\'_Pringles\', \'_Total\'))\n\n# Calculating market cap percentage\nmarket_cap_data[\'Market_Cap_Percent\'] = (market_cap_data[\'Sales_NIS_Pringles\'] / market_cap_data[\'Sales_NIS_Total\']) * 100\n\n# Getting the weekly market cap values\nweekly_market_cap = market_cap_data[[\'Day\', \'Market_Cap_Percent\']]\n\nanswer = f"הנתח השוק של פרינגלס בשבועות הוא:\\n{weekly_market_cap.to_string(index=False)}"\n```'},
 {'role': 'user',
  'content': 'מתי השבוע שנתח השוק של פרינגלס היה הכי נמוך מבין השבועות של מאי 24'},
 {'role': 'assistant',
  'content': '```python\n# Merging sales data with items data\nmerged_data = stnx_sales.merge(stnx_items, on=\'Barcode\', how=\'inner\')\n\n# Filtering for Pringles brand\npringles_data = merged_data[merged_data[\'Brand_Name\'] == \'פרינגלס\']\n\n# Grouping by week and calculating total sales in NIS for Pringles\npringles_weekly_sales = pringles_data.resample(\'W-Mon\', on=\'Day\').agg({\'Sales_NIS\': \'sum\'}).reset_index()\n\n# Calculating total sales for the snacks category\ntotal_weekly_sales = merged_data.resample(\'W-Mon\', on=\'Day\').agg({\'Sales_NIS\': \'sum\'}).reset_index()\n\n# Merging to calculate market cap\nmarket_cap_data = pringles_weekly_sales.merge(total_weekly_sales, on=\'Day\', suffixes=(\'_Pringles\', \'_Total\'))\n\n# Calculating market cap percentage\nmarket_cap_data[\'Market_Cap_Percent\'] = (market_cap_data[\'Sales_NIS_Pringles\'] / market_cap_data[\'Sales_NIS_Total\']) * 100\n\n# Finding the week with the lowest market cap\nlowest_market_cap_week = market_cap_data.loc[market_cap_data[\'Market_Cap_Percent\'].idxmin()]\n\n# Formatting the answer\nanswer = f"השבוע שבו נתח השוק של פרינגלס היה הכי נמוך הוא {lowest_market_cap_week[\'Day\'].strftime(\'%d/%m/%Y\')} עם נתח שוק של {lowest_market_cap_week[\'Market_Cap_Percent\']:.2f}%."\n```'},
 {'role': 'user',
  'content': 'מה היה המחיר הממוצע של פרינגלס בשבוע עם נתח השוק הגבוה ביותר'},
 {'role': 'assistant',
  'content': '```python\nimport pandas as pd\n\n# Filter for Pringles items\npringles_items = stnx_items[stnx_items[\'Brand_Name\'] == \'פרינגלס\']\n\n# Merge sales and items dataframes\nmerged_sales = pd.merge(stnx_sales, pringles_items[[\'Barcode\', \'Brand_Name\']], on=\'Barcode\')\n\n# Group by week and calculate total sales in NIS for Pringles and total sales for the category\nmerged_sales[\'Week\'] = merged_sales[\'Day\'].dt.isocalendar().week\nweekly_sales = merged_sales.groupby([\'Week\']).agg({\'Sales_NIS\': \'sum\'}).reset_index()\n\n# Calculate total sales for the snacks category for each week\ntotal_weekly_sales = stnx_sales.groupby(stnx_sales[\'Day\'].dt.isocalendar().week).agg({\'Sales_NIS\': \'sum\'}).reset_index()\ntotal_weekly_sales.columns = [\'Week\', \'Total_Sales_NIS\']\n\n# Merge to calculate market share\nweekly_sales = pd.merge(weekly_sales, total_weekly_sales, on=\'Week\', suffixes=(\'_Pringles\', \'_Total\'))\nweekly_sales[\'Market_Share\'] = weekly_sales[\'Sales_NIS_Pringle\'] / weekly_sales[\'Total_Sales_NIS\']\n\n# Find the week with the highest market share for Pringles\nmax_market_share_week = weekly_sales.loc[weekly_sales[\'Market_Share\'].idxmax(), \'Week\']\n\n# Filter sales data for Pringles during that week\npringles_week_data = merged_sales[merged_sales[\'Week\'] == max_market_share_week]\n\n# Calculate the average price per unit for Pringles during that week\naverage_price = pringles_week_data[\'Price_Per_Unit\'].mean()\n\nanswer = f\'הממוצע של המחיר של פרינגלס בשבוע עם נתח השוק הגבוה ביותר הוא {average_price:.2f} ש"ח.\'\n```'},
  {'role': 'user', 'content': 'מי המוצר הכי חזק של דיפלומט במכירות במאי 24'},
 {'role': 'assistant',
  'content': '```python\n# Filtering sales data for for May 2024\nstnx_sales_may = stnx_sales[(stnx_sales[\'Day\'].dt.month == 5) & (stnx_sales[\'Day\'].dt.year == 2024)]\n\n# Merging with items data to get product and supplier details\nstnx_sales_items = stnx_sales_may.merge(stnx_items, on=\'Barcode\', how=\'inner\')\n\n# Filtering sales data for the supplier \'דיפלומט\'\ndiplomat_sales_items = stnx_sales_items[stnx_sales_items.Supplier_Name==\'דיפלומט\']\n\n\n# Grouping by item name and summing sales units to identify the best-selling product\nbest_selling_product = diplomat_sales_items.groupby(\'Item_Name\').agg({\'Sales_Units\': \'sum\'}).reset_index()\n\n# Finding the product with the highest sales units\nstrongest_product = best_selling_product.loc[best_selling_product[\'Sales_Units\'].idxmax()]\n\nproduct_name = strongest_product[\'Item_Name\']\nsales_units = strongest_product[\'Sales_Units\']\n\nanswer = f"המוצר הכי חזק של דיפלומט במכירות במאי 2024 הוא \'{product_name}\' עם {sales_units} יחידות שנמכרו."\n```'},
  {'role': 'user', 'content':'מהם 5 המותגים הנמכרים ביותר אשר חוו עליה בנתח השוק בחודש אפריל? הצג לי את נתחי השוק של המותגים הללו בחודש אפריל בפילוח שבועי בטבלה לצד נתח השוק של פרינגלס'},
{'role': 'assistant', 
'content':'```python\n# Filtering sales data for April 2024\nstnx_sales_april = stnx_sales[(stnx_sales[\'Day\'].dt.month == 4) & (stnx_sales[\'Day\'].dt.year == 2024)]\n\n# Merging sales data with items data\nmerged_data_april = stnx_sales_april.merge(stnx_items, on=\'Barcode\', how=\'inner\')\n\n# Grouping by week and calculating total sales in NIS for each brand\nweekly_sales = merged_data_april.resample(\'W-Mon\', on=\'Day\').agg({\'Sales_NIS\': \'sum\'}).reset_index()\nweekly_sales[\'Week\'] = weekly_sales[\'Day\'].dt.isocalendar().week\n\n# Getting total weekly sales per brand\nbrand_weekly_sales = merged_data_april.groupby([\'Brand_Name\', pd.Grouper(key=\'Day\', freq=\'W-Mon\')])[\'Sales_NIS\'].sum().reset_index()\n\n# Pivoting to get each brand\'s sales in separate columns\nbrand_sales_pivot = brand_weekly_sales.pivot(index=\'Day\', columns=\'Brand_Name\', values=\'Sales_NIS\').fillna(0)\n\n# Calculating market share for each brand and for Pringles\ntotal_sales_per_week = brand_sales_pivot.sum(axis=1)\nmarket_shares = brand_sales_pivot.div(total_sales_per_week, axis=0) * 100\n\n# Finding brands that experienced an increase in market share\nmarket_share_changes = market_shares.diff().iloc[1:]  # Change from the first week\nincreased_share_brands = market_share_changes[market_share_changes > 0].index\n\n# Finding the top 5 brands with increased market share\ntop_brands = market_shares.loc[increased_share_brands].sum().nlargest(5)\n\n# Preparing the final result\nresult_data = market_shares[top_brands.index].copy()\nresult_data[\'פרינגלס\'] = market_shares[\'פרינגלס\']\n\nanswer = f"המותגים הנמכרים ביותר אשר חוו עליה בנתח השוק בחודש אפריל הם:\n{result_data.to_string()}"\n```'}
  ]


db_password = os.getenv('DB_PASSWORD')  
openai_api_key = os.getenv('OPENAI_KEY')

client = AzureOpenAI(  
    azure_endpoint="https://ai-usa.openai.azure.com/",  
    api_key=openai_api_key,  
    api_version="2024-02-15-preview"  
)  
MODEL = "Diplochat"  
  
def generate_text(prompt, sys_msg, examples=[]):  
    response = client.chat.completions.create(  
        model=MODEL,  # model = "deployment_name"  
        messages=[{"role": "system", "content": sys_msg}] + examples + [{"role": "user", "content": prompt}],  
        temperature=0.7,  
        max_tokens=2000,  
        top_p=0.95,  
        frequency_penalty=0,  
        presence_penalty=0,  
        stop=None  
    )  
    return response.choices[0].message.content.strip()  
  
  
  
def comment_out_lines(code,print_drop = True):  
    
    
    # Define the patterns and replacements  
    lines_to_comment = [  
        "stnx_sales, stnx_items, chp = load_data()"
    ]  
        
    if print_drop:
    # Replace any print() statements with #print()  
        code = re.sub(r"^(\s*)print\(", r"\1#print(", code, flags=re.MULTILINE)  
    
    # Define the pattern to find everything after any of the specified lines  
    pattern = re.compile(r'=(?: *load_data\(\) *|load_data\(\))(.*)', re.DOTALL)  
    # Search the text  
    match = pattern.search(code)

    # If a match is found, extract the part after the line  
    if match:  
        code = match.group(1)  
        if code.strip().startswith('()'):
            code = code[2:]
        code = code.strip()  
    else:  
        code = code     
    return code 
def insert_log_data(conn, log_session):  
    # Create the table if it does not exist  
    create_table_query = """  
    IF NOT EXISTS (  
        SELECT *   
        FROM INFORMATION_SCHEMA.TABLES   
        WHERE TABLE_NAME = 'AI_LOG'  
    )  
    BEGIN  
        CREATE TABLE AI_LOG (  
            ID INT IDENTITY(1,1) PRIMARY KEY,
            Conversation_ID NVARCHAR(100), 
            Timestamp DATETIME,  
            User_Name NVARCHAR(100),  
            User_Prompt NVARCHAR(MAX),  
            LLM_Responses NVARCHAR(MAX),  
            Code_Extractions NVARCHAR(MAX),  
            Final_Answer NVARCHAR(MAX),  
            Num_Attempts INT,  
            Num_LLM_Calls INT,  
            Errors NVARCHAR(MAX),  
            Total_Time FLOAT,
            User_Ratings NVARCHAR(MAX)
        )  
    END  
    """  
      
    cursor = conn.cursor()  
    cursor.execute(create_table_query)  
      
    # Insert log data into the AI_LOG table  
    insert_query = """  
    INSERT INTO AI_LOG (Conversation_ID, Timestamp, User_Name, User_Prompt, LLM_Responses, Code_Extractions, Final_Answer, Num_Attempts, Num_LLM_Calls, Errors, Total_Time, User_Ratings)  
    VALUES (?,?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)  
    """  
      
    cursor.execute(insert_query, log_session)  
    conn.commit()  
    cursor.close()

def alter_log_data(conn, prompt_timestamp, user_feedback):  
    try:  
        cursor = conn.cursor()  
  
        # Define the SQL update command using parameterized query  
        sql_update_query = """  
        UPDATE [dbo].[AI_LOG]  
        SET [User_Ratings] = ?  
        WHERE [Timestamp] = ?;  
        """  
  
        # Execute the update command with parameters  
        cursor.execute(sql_update_query, (user_feedback, prompt_timestamp))  
  
        # Commit the transaction  
        conn.commit()  
  
        # print("User rating updated successfully.")  
  
    except Exception as e:  
        print("Error occurred:", e)  
  
    finally:  
        # Close the cursor  
        cursor.close()     



@st.cache_data(show_spinner="Loading data.. this can take a few minutes, feel free to grab a coffee ☕") 
def load_data():  
    conn = pyodbc.connect(driver='{ODBC Driver 17 for SQL Server}',  
                          server='diplomat-analytics-server.database.windows.net',  
                          database='NBO-DB',  
                          uid='analyticsadmin', pwd=db_password)  
  

    #Define tables and queries
    tables = {
        'DW_FACT_STORENEXT_BY_INDUSTRIES_SALES': """
            SELECT Day, Barcode, Format_Name, Sales_NIS, Sales_Units, Price_Per_Unit
            FROM [dbo].[DW_FACT_STORENEXT_BY_INDUSTRIES_SALES]
            WHERE Day BETWEEN '2023-12-31' AND '2024-09-01'
        """,
        'DW_DIM_STORENEXT_BY_INDUSTRIES_ITEMS': """
            SELECT Barcode, Item_Name, Category_Name, Sub_Category_Name, Brand_Name, Sub_Brand_Name, Supplier_Name
            FROM [dbo].[DW_DIM_STORENEXT_BY_INDUSTRIES_ITEMS]
        """,
        'DW_CHP_AGGR': """
            SELECT DATE,BARCODE,CHAIN,AVG_PRICE,AVG_SELLOUT_PRICE,SELLOUT_DESCRIPTION,NUMBER_OF_STORES
            FROM [dbo].[DW_CHP_AGGR]
            WHERE DATE BETWEEN '2023-12-31' AND '2024-09-01'
        """
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

    def extract_code(txt):  
        pattern = r'python(.*?)'  
        all_code = re.findall(pattern, txt, re.DOTALL)  
        if len(all_code) == 1:  
            final_code = all_code[0]  
        else:  
            final_code = '\n'.join(all_code)  
        return final_code
    
    admin_list = ['Yonatan Rabinovich']
    user_name = st.session_state.get("name", "Guest")  # Default to "Guest" if not set

    st.title(f"{user_name} Sales Copilot 🤖")  

    dataframes = load_data()  
    
    # Assigning dataframes to variables
    stnx_sales = dataframes['DW_FACT_STORENEXT_BY_INDUSTRIES_SALES']
    stnx_items = dataframes['DW_DIM_STORENEXT_BY_INDUSTRIES_ITEMS']
    chp = dataframes['DW_CHP_AGGR']

    # Convert date columns to datetime
    stnx_sales['Day'] = pd.to_datetime(stnx_sales['Day'])
    chp['DATE'] = pd.to_datetime(chp['DATE'])

    user_avatar = '🧑'

    client = AzureOpenAI(  
        azure_endpoint="https://ai-usa.openai.azure.com/",  
        api_key=openai_api_key,  
        api_version="2024-02-15-preview"  
    )  
    MODEL = "Diplochat"  
    
    # base_history = [{"role": "system", "content": sys_msg}]+examples

    # Initialize log_dfs 
    if 'log_dfs' not in st.session_state:  
        st.session_state.log_dfs = []  
    
    if "openai_model" not in st.session_state:  
        st.session_state["openai_model"] = MODEL 
    
    if 'user_feedback' not in st.session_state:  
        st.session_state.user_feedback = ''

    if 'user_feedback_lst' not in st.session_state:
        st.session_state.user_feedback_lst = []

    if "messages" not in st.session_state:  
        st.session_state.messages = [{"role": "system", "content": sys_msg}]

    if "base_history" not in st.session_state:  
        st.session_state.base_history = [{"role": "system", "content": sys_msg}]+examples




    def handle_feedback():
        conn = pyodbc.connect(driver='{ODBC Driver 17 for SQL Server}',  
                        server='diplomat-analytics-server.database.windows.net',  
                        database='NBO-DB',  
                        uid='analyticsadmin', pwd=db_password)    
        
        st.session_state.user_feedback_lst.append(st.session_state.user_feedback)
        st.session_state.log_dfs = st.session_state.log_dfs[:-1]
        # add to log frame
        tmp_df.loc[0,'User_Ratings'] = str(st.session_state.user_feedback)
        st.session_state.log_dfs.append(tmp_df)
        # add to sql
        # Call the function  
        alter_log_data(conn, tmp_df.loc[0,'Timestamp'], str(st.session_state.user_feedback))  
    
        # Close the connection  
        conn.close() 
        st.toast("✔️ Feedback received!")
        
    answer = ''


    # Display chat messages from history on app rerun  
    for message in st.session_state.messages:  
        if message["role"] == 'assistant':  
            with st.chat_message(message["role"], avatar='🤖'):  
                # display_txt = f"{message["content"]} user feedback: {st.session_state.user_feedback} last feedbacks {st.session_state.user_feedback_lst}" 
                # display_txt = message["content"]+f' history_length: {len(st.session_state.base_history)}'
                # st.markdown(display_txt)
                st.markdown(message["content"])

        elif message["role"] == 'user':  
            with st.chat_message(message["role"], avatar=user_avatar):  
                st.markdown(message["content"])  


    log_data = []
    # data in each session: prompt,txt_content,code_lst,
    log_session = []

    log_cols = ['Conversation_ID','Timestamp','User_Name','User_Prompt','LLM_Responses','Code_Extractions','Final_Answer','Num_Attempts','Num_LLM_Calls','Errors','Total_Time','User_Ratings']
    log_dfs = []

    israel_tz = pytz.timezone("Asia/Jerusalem")
    
    now = datetime.now(israel_tz).strftime("%Y-%m-%d %H:%M:%S") 
    name_id = ''.join(i for i in user_name if i==i.upper()).replace(' ','')
    ts_id = ''.join(i for i in now if i.isdigit())

    conv_id = f'{name_id}_{ts_id}'
    # Initialize the conversation ID in session state if it doesn't exist  
    if 'conv_id' not in st.session_state:  
        st.session_state.conv_id = f'{name_id}_{ts_id}'


    m_limit = 25
    if 'memory_limit' not in st.session_state:
        st.session_state.memory_limit = m_limit
        
    if prompt := st.chat_input("Ask me anything"):
        prompt_timestamp = datetime.now(israel_tz).strftime("%Y-%m-%d %H:%M:%S") 
        st.session_state.messages.append({"role": "user", "content": prompt})
        st.session_state.base_history.append({"role": "user", "content": prompt})
        
        with st.chat_message("user", avatar=user_avatar):
            st.markdown(prompt)

            start_time = time.time()
        

        with st.spinner("Thinking..."):
            
            answer = ''
            txt = ''
            
            max_attempts = 10
            errors = []
            attempts = 1
            txt_content_lst = []
            code_lst = []
            n_llm_api_call = 0


            while attempts < max_attempts:
                
                try:

                    txt = client.chat.completions.create(
                        model=st.session_state["openai_model"],
                        messages=[
                            {"role": m["role"], "content": m["content"]}
                            for m in st.session_state.base_history
                        ],
                        max_tokens=2000,
                        stream=False,
                    )
                    txt_content = txt.choices[0].message.content
                    
                    n_llm_api_call+=1
                    # append original gen ai content to the list
                    txt_content_lst.append(txt_content)
                
                    # Regex pattern to extract the Python code
                    pattern = r'```python(.*?)```'   
                    all_code = re.findall(pattern, txt_content, re.DOTALL)
                    if len(all_code) == 1:  
                        code = all_code[0]
                        
                    else:  
                        code = '\n'.join(all_code)              
                    
                
                    # Use re.sub to comment out any print statement  
                    code = re.sub(r"^(\s*)print\(", r"\1#print(", code, flags=re.MULTILINE)
                    # Use re.sub to comment out any import statement  
                    code = re.sub(r"^(\s*)import\s", r"\1#import ", code, flags=re.MULTILINE)  
                    
                    local_context = {'chp':chp,'stnx_sales':stnx_sales,'stnx_items':stnx_items,'pd':pd,'SARIMAX':SARIMAX}
                    exec(code, {}, local_context)
                    answer = local_context.get('answer', "No answer found.") 
                    
                    if answer == "No answer found.":  
                        raise ValueError("No answer found.")  
                    
                    # sys_decorator = """
                    # You are an AI assistant created to enhance the aesthetic quality of LLM responses. 
                    # Your task is to take the generated response and find ways to make it more articulate and visually appealing, 
                    # while preserving the original numbers and facts, ensuring that the output resembles that of a language model.
                    # """
                    sys_decorator = """
                    You are an AI assistant designed to enhance the quality and presentation of responses from the perspective of Diplomat Distributors Ltd. 
                    Your task is to refine generated content, making it more articulate and visually appealing, 
                    while preserving all original numbers and facts. Ensure that the output reflects a professional tone suitable 
                    for internal discussions and communications within the company.
                    """

                    answer = generate_text(answer, sys_decorator)
                    n_llm_api_call+=1

                    history_msg = f"```python{code}```"

                    with st.chat_message("assistant", avatar='🤖'):
                        # Create a placeholder for streaming output  
                        placeholder = st.empty()  
                        streamed_text = ""  
                        
                        # Stream the answer output  
                        for char in answer:  
                            streamed_text += char  
                            placeholder.markdown(streamed_text)  
                            time.sleep(0.01)  # Adjust the sleep time to control the streaming speed 

                    st.session_state.base_history.append({"role": "assistant", "content": history_msg})
                    
                    # append regex formatted code with python markdown content to the list
                    code_lst.append(history_msg)
                    break

                except Exception as e:  
                    errors.append(f"Attempt {attempts} failed: {e}")  
                    attempts += 1
                    
            # generate anwer for failures
            if attempts == max_attempts:
                # replace with ai generated text
                answer = generate_text(prompt, sys_error)
                n_llm_api_call+=1
                with st.chat_message("assistant", avatar='🤖'):
                    # Create a placeholder for streaming output  
                    placeholder = st.empty()  
                    streamed_text = ""  
                    
                    # Stream the answer output  
                    for char in answer:  
                        streamed_text += char  
                        placeholder.markdown(streamed_text)  
                        time.sleep(0.01)  # Adjust the sleep time to control the streaming speed
                
                # delete the last question from base_history
                st.session_state.base_history = st.session_state.base_history[:-1]

            st.session_state.messages.append({"role": "assistant", "content": answer})
            
            # memory control
            if len(st.session_state.base_history)>st.session_state.memory_limit:
                # delete the first 2 massages after system
                st.session_state.base_history = [st.session_state.base_history[0]]+st.session_state.base_history[3:]

            elapsed_time = time.time() - start_time

            # append rest of the data from the session
            empty_rating = ''

            log_session.append(st.session_state.conv_id)
            log_session.append(prompt_timestamp)
            log_session.append(user_name)
            log_session.append(prompt)
            log_session.append(str(txt_content_lst))
            log_session.append(str(code_lst))
            log_session.append(answer)
            log_session.append(attempts)
            log_session.append(n_llm_api_call)
            log_session.append(str(errors))
            log_session.append(elapsed_time)
            log_session.append(empty_rating)
            
        log_data.append(log_session)
        
        # Insert log data into SQL table  
        conn = pyodbc.connect(driver='{ODBC Driver 17 for SQL Server}',  
                              server='diplomat-analytics-server.database.windows.net',  
                              database='NBO-DB',  
                              uid='analyticsadmin', pwd=db_password)  
        insert_log_data(conn, log_session)  
        conn.close()  
        
        tmp_df = pd.DataFrame(log_data,columns=log_cols)
        # log_dfs.append(tmp_df)

        st.session_state.log_dfs.append(tmp_df)
        
        
        # log_df = pd.concat(log_dfs,axis=0).reset_index(drop = True)
        log_df = pd.concat(st.session_state.log_dfs, axis=0).reset_index(drop=True)
        

        if user_name in admin_list:
            # Create an expander  
            with st.expander("Show Log DataFrame"):  
                # Your code inside the expander  
                st.dataframe(log_df)
                st.markdown(st.session_state.base_history)
            
        # Feedback mechanism


        with st.form('form'):
            streamlit_feedback(feedback_type="thumbs",
                                optional_text_label="Enter your feedback here", 
                                align="flex-start", 
                                key='user_feedback')
            st.form_submit_button('Save feedback', on_click=handle_feedback)