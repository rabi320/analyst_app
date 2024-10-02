import streamlit as st  
import streamlit_authenticator as stauth  
from streamlit_feedback import streamlit_feedback
import yaml
from yaml.loader import SafeLoader
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

import os
import numpy as np
import tiktoken
from pyluach import dates  


# Suppress all warnings  
warnings.filterwarnings('ignore')   

# from st_pages import hide_pages
  
with open('config.yaml') as file:
    config = yaml.load(file, Loader=SafeLoader)

authenticator = stauth.Authenticate(
    config['credentials'],
    config['cookie']['name'],
    config['cookie']['key'],
    config['cookie']['expiry_days'],
    config['pre-authorized']
)

st.title('Diplomat AI')

# Clear the cache on app start or refresh
if 'cache_cleared' not in st.session_state:
    st.cache_data.clear()
    st.session_state.cache_cleared = True

# Login widget  
authentication_status = authenticator.login()  

# hide_pages([ "Map"])

# Adjusted authentication status handling  
if st.session_state['authentication_status']:  
    
    st.sidebar.markdown("![](https://www.diplomat-global.com/wp-content/uploads/2018/06/logo.png)")
    authenticator.logout(location = 'sidebar')  # Add logout functionality  
    st.write(f'Welcome *{st.session_state["name"]}*')  # Display welcome message  



    if 'resolution_type' not in st.session_state:
        st.session_state.resolution_type = "weekly"  # default value

    
    
    # Sidebar radio button for choosing resolution type
    selected_resolution = st.sidebar.radio("Choose resolution:", ["weekly", "monthly"], index=0 if st.session_state.resolution_type == "weekly" else 1)

    # Check if the resolution type has changed and rerun/cache if it has
    if selected_resolution != st.session_state.resolution_type:
        st.session_state.resolution_type = selected_resolution


    if 'chp_or_invoices' not in st.session_state:
            st.session_state.chp_or_invoices = "invoices"  # default value

    # Sidebar radio button for choosing chp or invoices 
    chp_or_invoices = st.sidebar.radio("Choose data source:", ["invoices", "chp"], index=0 if st.session_state.chp_or_invoices == "invoices" else 1)

    # Check if the resolution type has changed and rerun/cache if it has
    if chp_or_invoices != st.session_state.chp_or_invoices:
        st.session_state.chp_or_invoices = chp_or_invoices




    #####################
    # diplochat analyst #
    #####################


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

    4. **DATE_HOLIAY_DATA** ('dt_df')
    - **Description**: This fact table records daily holiday data and hebrew dates.  
    - **Columns**:
        - `DATE`: Date (datetime).    
        - 'HEBREW_DATE': Hebrew date as string.
        - 'HOLIDAY': the name of the holiday or null if no holiday is on that date (string).
        - **Note**: this data is from a python process involving a package of hebrew dates and holidays. 

    5. **AGGR_MONTHLY_DW_INVOICES** ('inv_df'):
        - **Description**: This fact table records Diplomat's invoice data.
        - **Columns**:
        - `DATE`: Date (datetime). 
        - `SALES_ORGANIZATION_CODE`: The id of Diplomat's buisness unit.
        - `MATERIAL_CODE`: The id of Diplomat's items.
        - `INDUSTRY_CODE`:  The id of Diplomat's different industries that relate to their customers.
        - 'CUSTOMER_CODE': The id of the exact customers.
        - 'Gross': The gross sales.
        - 'Net': The net sales.
        - 'Net VAT': The net sales with tax.
        - 'Gross VAT': The gross sales with tax.
        - 'Units': the number of units.
        - **Note**: this data relates to the sell in of diplomat and needs the material barcode from the material table to connect to external data like chp and others. 

        

        
    this is the code that already loaded the data to the IDE:

    ```python
    db_password = os.getenv('DB_PASSWORD')
    def load_data():  
        conn = pyodbc.connect(driver='{ODBC Driver 17 for SQL Server}',  
                            server='diplomat-analytics-server.database.windows.net',  
                            database='Diplochat-DB',  
                            uid='analyticsadmin', pwd=db_password)  
    

        #Define tables and queries
        tables = {
            'DW_FACT_STORENEXT_BY_INDUSTRIES_SALES': \"\"\"
                SELECT Day, Barcode, Format_Name, Sales_NIS, Sales_Units, Price_Per_Unit
                FROM [dbo].[DW_FACT_STORENEXT_BY_INDUSTRIES_SALES]
                WHERE Day BETWEEN '2023-12-31' AND '2024-09-01'
            \"\"\",
            'DW_DIM_STORENEXT_BY_INDUSTRIES_ITEMS': \"\"\"
            [Query]
            \"\"\",
            'DW_CHP_AGGR': \"\"\"
            [Query]
            \"\"\"
            ,
            'AGGR_MONTHLY_DW_INVOICES':\"\"\"
            [Query]
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

    dt_df = create_date_dataframe(start_date, end_date)
    dataframes['DATE_HOLIAY_DATA'] = dt_df

    # Assigning dataframes to variables
    stnx_sales = dataframes['DW_FACT_STORENEXT_BY_INDUSTRIES_SALES']
    stnx_items = dataframes['DW_DIM_STORENEXT_BY_INDUSTRIES_ITEMS']
    chp = dataframes['DW_CHP_AGGR']
    dt_df = dataframes['DATE_HOLIAY_DATA']
    inv_df = dataframes['AGGR_MONTHLY_DW_INVOICES']

    # Convert date columns to datetime
    stnx_sales['Day'] = pd.to_datetime(stnx_sales['Day'])
    chp['DATE'] = pd.to_datetime(chp['DATE'])
    dt_df['DATE'] = pd.to_datetime(dt_df['DATE'])
    inv_df['DATE] = pd.to_datetime(inv_df['DATE'])
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
    sys_msg += f'\nYour operation present date is {datetime.now()}.'

    sys_error = """  
    You are an assistant that informs the user when their input is unclear,  
    and you ask them to provide more details or rephrase their message in the same language they used.  
    """  

    examples = [{'role': 'user', 'content': 'מהם ניתחי השוק של מותג פרינגלס בפילוח שבועי'},
    {'role': 'assistant',
    'content': '```python\n# Merging sales data with items data\nmerged_data = stnx_sales.merge(stnx_items, on=\'Barcode\', how=\'inner\')\n\n# Filtering for Pringles brand\npringles_data = merged_data[merged_data[\'Brand_Name\'] == \'פרינגלס\']\n\n# Grouping by week and calculating total sales in NIS for Pringles\npringles_weekly_sales = pringles_data.resample(\'W\', on=\'Day\').agg({\'Sales_NIS\': \'sum\'}).reset_index()\n\n# Get the brand category\ncategory_for_market_cap = pringles_data.Category_Name.values[0]\n\n# Filter by the brand category\nmerged_data_category = merged_data[merged_data.Category_Name==category_for_market_cap]\n\n# Calculating total sales for the category\ntotal_weekly_sales = merged_data_category.resample(\'W\', on=\'Day\').agg({\'Sales_NIS\': \'sum\'}).reset_index()\n\n# Merging to calculate market cap\nmarket_cap_data = pringles_weekly_sales.merge(total_weekly_sales, on=\'Day\', suffixes=(\'_Pringles\', \'_Total\'))\n\n# Calculating market cap percentage\nmarket_cap_data[\'Market_Cap_Percent\'] = (market_cap_data[\'Sales_NIS_Pringles\'] / market_cap_data[\'Sales_NIS_Total\']) * 100\n\n# Getting the weekly market cap values\nweekly_market_cap = market_cap_data[[\'Day\', \'Market_Cap_Percent\']]\n\nanswer = f"הנתח השוק של פרינגלס בשבועות הוא:\\n{weekly_market_cap.to_string(index=False)}"\n```'},
    {'role': 'user','content': 'מתי השבוע שנתח השוק של פרינגלס היה הנמוך ביותר'},
    {'role': 'assistant',
    'content': '```python\n# Merging sales data with items data\nmerged_data = stnx_sales.merge(stnx_items, on=\'Barcode\', how=\'inner\')\n\n# Filtering for Pringles brand\npringles_data = merged_data[merged_data[\'Brand_Name\'] == \'פרינגלס\']\n\n# Grouping by week and calculating total sales in NIS for Pringles\npringles_weekly_sales = pringles_data.resample(\'W\', on=\'Day\').agg({\'Sales_NIS\': \'sum\'}).reset_index()\n\n# Get the brand category\ncategory_for_market_cap = pringles_data.Category_Name.values[0]\n\n# Filter by the brand category\nmerged_data_category = merged_data[merged_data.Category_Name==category_for_market_cap]\n\n# Calculating total sales for the category\ntotal_weekly_sales = merged_data_category.resample(\'W\', on=\'Day\').agg({\'Sales_NIS\': \'sum\'}).reset_index()\n\n# Merging to calculate market cap\nmarket_cap_data = pringles_weekly_sales.merge(total_weekly_sales, on=\'Day\', suffixes=(\'_Pringles\', \'_Total\'))\n\n# Calculating market cap percentage\nmarket_cap_data[\'Market_Cap_Percent\'] = (market_cap_data[\'Sales_NIS_Pringles\'] / market_cap_data[\'Sales_NIS_Total\']) * 100\n\n# Finding the week with the lowest market cap\nlowest_market_cap_week = market_cap_data.loc[market_cap_data[\'Market_Cap_Percent\'].idxmin()]\n\n# Formatting the answer\nanswer = f"השבוע שבו נתח השוק של פרינגלס היה הכי נמוך הוא {lowest_market_cap_week[\'Day\'].strftime(\'%d/%m/%Y\')} עם נתח שוק של {lowest_market_cap_week[\'Market_Cap_Percent\']:.2f}%."\n```'},
    {'role': 'user','content': 'תתן לי 8 דוגמאות אקראיות של תיאורי מבצעים ייחודיים של המתחרים של מותג פרינגלס ושם המוצר שלהם באותה תקופה כרשימה'},
    {'role': 'assistant',
    'content': '```python\n# Filtering promotional sales only\npromotional_sales = chp[chp[\'AVG_SELLOUT_PRICE\'].notnull() & (chp[\'AVG_SELLOUT_PRICE\'] > 0)]\n\n# Merging with items data to get product names and supplier information\npromotional_data = promotional_sales.merge(stnx_items, left_on=\'BARCODE\', right_on=\'Barcode\', how=\'inner\')\n\n# Get the brand category and competition, then Filter promotional data by the competition (not diplomat) and the brand category\nbrand_category = promotional_data[promotional_data[\'Brand_Name\'] == \'פרינגלס\'].Category_Name.values[0]\npromotional_data = promotional_data[(promotional_data[\'Supplier_Name\'] != \'דיפלומט\')&(promotional_data[\'Category_Name\']==brand_category)]\n\n# Selecting unique promotional descriptions and product names\nunique_promotions = promotional_data[[\'SELLOUT_DESCRIPTION\', \'Item_Name\', \'Supplier_Name\']].drop_duplicates(subset = [\'SELLOUT_DESCRIPTION\'])\n\n# Taking random 8 unique promotions\nunique_promotions_list = unique_promotions.sample(8).to_dict(orient=\'records\')\n\n# Formatting the output\npromotion_examples = []\nfor promotion in unique_promotions_list:\n promotion_text = f"מוצר: {promotion[\'Item_Name\']} - תיאור מבצע: {promotion[\'SELLOUT_DESCRIPTION\']} (ספק: {promotion[\'Supplier_Name\']})"\n promotion_examples.append(promotion_text)\n\nanswer = "\\n".join(promotion_examples)\n```'}
    ]

    db_password = os.getenv('DB_PASSWORD')  
    openai_api_key = os.getenv('OPENAI_KEY')

    client = AzureOpenAI(  
        azure_endpoint="https://ai-usa.openai.azure.com/",  
        api_key=openai_api_key,  
        api_version="2024-02-15-preview"  
    )

    MODEL = "Diplochat"  


    def model_reponse(prompt, sys_msg, examples=[]):  
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

        return response  


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
                User_Ratings NVARCHAR(MAX),
                Usage NVARCHAR(MAX)
            )  
        END  
        """  
        
        cursor = conn.cursor()  
        cursor.execute(create_table_query)  
        
        # Insert log data into the AI_LOG table  
        insert_query = """  
        INSERT INTO AI_LOG (Conversation_ID, Timestamp, User_Name, User_Prompt, LLM_Responses, Code_Extractions, Final_Answer, Num_Attempts, Num_LLM_Calls, Errors, Total_Time, User_Ratings, Usage)  
        VALUES (?,?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)  
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

    def gregorian_to_hebrew(year, month, day):  
        return dates.GregorianDate(year, month, day).to_heb().hebrew_date_string()  
    
    def get_jewish_holidays(year, month, day):  
        return dates.GregorianDate(year, month, day).festival(hebrew=True)  
    
    def create_date_dataframe(start_date, end_date):  
        # Create a DataFrame with the date range  
        dt_df = pd.DataFrame({'DATE': pd.date_range(start=start_date, end=end_date)})  
        
        # Extract Hebrew dates and Jewish holidays  
        dt_df['HEBREW_DATE'] = dt_df['DATE'].apply(lambda x: gregorian_to_hebrew(x.year, x.month, x.day))  
        dt_df['HOLIDAY'] = dt_df['DATE'].apply(lambda x: get_jewish_holidays(x.year, x.month, x.day))

        
        return dt_df  

    @st.cache_data(show_spinner="Loading data.. this can take a few minutes, feel free to grab a coffee ☕") 
    def load_data(resolution_type,chp_or_invoices):  
        conn = pyodbc.connect(driver='{ODBC Driver 17 for SQL Server}',  
                            server='diplomat-analytics-server.database.windows.net',  
                            database='Diplochat-DB',  
                            uid='analyticsadmin', pwd=db_password)  
        res_tp = resolution_type
        coi = chp_or_invoices

        #Define tables and queries
        tables = {
            f'AGGR_{res_tp.upper()}_DW_FACT_STORENEXT_BY_INDUSTRIES_SALES': f"""
                SELECT Day, Barcode, Format_Name, Sales_NIS, Sales_Units, Price_Per_Unit
                FROM [dbo].[AGGR_{res_tp.upper()}_DW_FACT_STORENEXT_BY_INDUSTRIES_SALES]
            """,
            'DW_DIM_STORENEXT_BY_INDUSTRIES_ITEMS': """
                SELECT Barcode, Item_Name, Category_Name, Sub_Category_Name, Brand_Name, Sub_Brand_Name, Supplier_Name
                FROM [dbo].[DW_DIM_STORENEXT_BY_INDUSTRIES_ITEMS]
            """,
            f'AGGR_{res_tp.upper()}_DW_CHP': f"""
                SELECT DATE,BARCODE,CHAIN,AVG_PRICE,AVG_SELLOUT_PRICE,SELLOUT_DESCRIPTION,NUMBER_OF_STORES
                FROM [dbo].[AGGR_{res_tp.upper()}_DW_CHP]
                WHERE [DATE] BETWEEN DATEADD(DAY, -90, GETDATE()) AND GETDATE()
            """,
            'AGGR_MONTHLY_DW_INVOICES':
            """
            SELECT [DATE]
                ,[SALES_ORGANIZATION_CODE]
                ,[MATERIAL_CODE]
                ,[INDUSTRY_CODE]
                ,[CUSTOMER_CODE]
                ,[Gross]
                ,[Net]
                ,[Net VAT]
                ,[Gross VAT]
                ,[Units]
            FROM [dbo].[AGGR_MONTHLY_DW_INVOICES]
            """,
            'AI_LOG':"""
            SELECT [ID]
                ,[Conversation_ID]
                ,[Timestamp]
                ,[User_Name]
                ,[User_Prompt]
                ,[LLM_Responses]
                ,[Code_Extractions]
                ,[Final_Answer]
                ,[Num_Attempts]
                ,[Num_LLM_Calls]
                ,[Errors]
                ,[Total_Time]
                ,[User_Ratings]
                ,[Usage]
            FROM [dbo].[AI_LOG]
            """
        }

        # Filter the tables based on the coi variable  
        filtered_tables = {  
            key: value for key, value in tables.items()   
            if key != 'AGGR_MONTHLY_DW_INVOICES' and coi == 'chp' or  
            key != f'AGGR_{res_tp.upper()}_DW_CHP' and coi == 'invoices'  
        }  


        dataframes = {}  
        for table, query in filtered_tables.items():  
            chunks = []  
            chunk_size = 10000  
            total_rows = pd.read_sql_query(f"SELECT COUNT(*) FROM ({query}) AS count_query", conn).iloc[0, 0]  
            total_chunks = (total_rows // chunk_size) + 1  
    
            for i, chunk in enumerate(pd.read_sql_query(query, conn, chunksize=chunk_size)):  
                chunks.append(chunk)  
            
            df = pd.concat(chunks, ignore_index=True)  
            dataframes[table] = df  
        conn.close()

        start_date = dataframes[f'AGGR_{res_tp.upper()}_DW_FACT_STORENEXT_BY_INDUSTRIES_SALES']['Day'].min()
        end_date = dataframes[f'AGGR_{res_tp.upper()}_DW_FACT_STORENEXT_BY_INDUSTRIES_SALES']['Day'].max()
        
        dt_df = create_date_dataframe(start_date, end_date)
        dataframes['DATE_HOLIAY_DATA'] = dt_df
        return dataframes  

    # Function to check if the text contains Hebrew characters
    def is_hebrew(text):
        return any('\u0590' <= char <= '\u05FF' for char in text)

    # similariry for dynamic examples
    # Initialize tokenizer  
    enc = tiktoken.encoding_for_model("gpt-4o-mini")  

    def binirizer_vectors(a, b):  
        # Create a combined set of unique elements from both lists  
        ab = np.array(list(set(a + b)))  
        
        # Create binary vectors for each input list  
        a_binary = np.array([1 if i in a else 0 for i in ab])  
        b_binary = np.array([1 if i in b else 0 for i in ab])  
        
        return a_binary, b_binary  
    
    def cosine_similarity(a, b):  
        return np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b))  
    
    def cosine_binarizer(a, b, encoder):
        # encode using encoder
        a, b = encoder.encode(a),encoder.encode(b)

        # Get binary vectors for the input lists  
        a_binary, b_binary = binirizer_vectors(a, b)  
        
        # Calculate and return the cosine similarity  
        return cosine_similarity(a_binary, b_binary)  

    def get_top_similar_prompts(log_df, user_prompt, top_n=3, current_user='Yonatan Rabinovich', date_from = datetime(2024,9,16)):  
        # Filter for liked examples  
        liked_example_cond = log_df.User_Ratings.str.contains('👍')  
    
        # If a current user is provided, filter by user name  
        user_cond = log_df.User_Name == current_user if current_user else True  
        
        date_from_cond = log_df.Timestamp>=date_from

        # Combine conditions  
        conds = liked_example_cond & user_cond  & date_from_cond
        cond_log_df = log_df[conds].copy().reset_index(drop=True)  
    
        # Calculate similarities  
        similarities = [cosine_binarizer(row['User_Prompt'], user_prompt, enc) for index, row in cond_log_df.iterrows()]  
    
        # Get top `top_n` similar prompts  
        top_indices = np.argsort(similarities)[-top_n:]#[::-1]  
        dynammic_examples = cond_log_df.loc[top_indices].copy()  
    
        # Prepare the list of dynamic examples  
        dynammic_examples_lst = []  
        
        for _, x in dynammic_examples.iterrows():  
            user_role = {'role': 'user', 'content': x['User_Prompt']}  
            assistant_role = {'role': 'assistant', 'content': eval(x['Code_Extractions'])[0]}  
            dynammic_examples_lst.append(user_role)  
            dynammic_examples_lst.append(assistant_role)  
    
        return dynammic_examples_lst
    
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
    
    # ensure weekly is default
    res_tp = st.session_state.get('resolution_type','weekly')
    
    # ensure chp is default
    coi = st.session_state.get('chp_or_invoices','invoices')
    
    st.title(f"{user_name} {res_tp.capitalize()} Sales Copilot 🤖")  
    
    # # Rerun button logic in the sidebar
    # if st.sidebar.button("Reload Data"):
    #     st.session_state['refresh'] = True
    #     st.rerun()  # This will rerun the whole app

    dataframes = load_data(res_tp,coi)  
    
    # Assigning dataframes to variables
    stnx_sales = dataframes[f'AGGR_{res_tp.upper()}_DW_FACT_STORENEXT_BY_INDUSTRIES_SALES']
    stnx_items = dataframes['DW_DIM_STORENEXT_BY_INDUSTRIES_ITEMS']
    dt_df = dataframes['DATE_HOLIAY_DATA']
    log_df = dataframes['AI_LOG']

    # Convert date columns to datetime
    stnx_sales['Day'] = pd.to_datetime(stnx_sales['Day'])
    dt_df['DATE'] = pd.to_datetime(dt_df['DATE'])

    #optional data
    if coi=='chp':
        chp = dataframes[f'AGGR_{res_tp.upper()}_DW_CHP']
        chp['DATE'] = pd.to_datetime(chp['DATE'])
    else:
        inv_df = dataframes['AGGR_MONTHLY_DW_INVOICES']
        inv_df['DATE'] = pd.to_datetime(inv_df['DATE'])    


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
                        database='Diplochat-DB',  
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
                # rtl
                # if is_hebrew(message["content"]):
                #     f0string = f'<div style="direction: rtl; text-align: right;">{message["content"]}</div>'
                #     st.markdown(f0string, unsafe_allow_html=True)
                # else:                       
                st.markdown(message["content"])

        elif message["role"] == 'user':  
            with st.chat_message(message["role"], avatar=user_avatar):  
                # rtl
                # if is_hebrew(message["content"]):
                #     f0string = f'<div style="direction: rtl; text-align: right;">{message["content"]}</div>'
                #     st.markdown(f0string, unsafe_allow_html=True)
                # else:                       
                st.markdown(message["content"])


    log_data = []
    # data in each session: prompt,txt_content,code_lst,
    log_session = []

    log_cols = ['Conversation_ID','Timestamp','User_Name','User_Prompt','LLM_Responses','Code_Extractions','Final_Answer','Num_Attempts','Num_LLM_Calls','Errors','Total_Time','User_Ratings','Usage']
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

    n_most_similar = 3
    if 'n_most_similar' not in st.session_state:
        st.session_state.n_most_similar = n_most_similar
        
    if prompt := st.chat_input("Ask me anything"):
        prompt_timestamp = datetime.now(israel_tz).strftime("%Y-%m-%d %H:%M:%S") 
        st.session_state.messages.append({"role": "user", "content": prompt})
        
        dynamic_examples_lst = get_top_similar_prompts(log_df, prompt, top_n=st.session_state.n_most_similar)

        
        st.session_state.base_history[1:1+st.session_state.n_most_similar*2] = dynamic_examples_lst
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

            usage_dict = {}
            py_attempts_usage_lst = []

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

                    py_usage_dict = txt.to_dict()['usage']
                    py_attempts_usage_lst.append(py_usage_dict)


                    
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
                    
                    if coi=='chp':
                        local_context = {'chp':chp,'stnx_sales':stnx_sales,'stnx_items':stnx_items,'pd':pd,'np':np,'dt_df':dt_df,'SARIMAX':SARIMAX}
                    else:
                        local_context = {'stnx_sales':stnx_sales,'stnx_items':stnx_items,'pd':pd,'np':np,'dt_df':dt_df,'SARIMAX':SARIMAX,'inv_df':inv_df}

                    exec(code, {}, local_context)
                    answer = local_context.get('answer', "No answer found.") 
                    
                    if answer == "No answer found.":  
                        raise ValueError("No answer found.")  
                    
                    sys_decorator = """
                    You are an AI assistant designed to enhance the quality and presentation of responses from the perspective of Diplomat Distributors Ltd. 
                    Your task is to refine generated content, making it more articulate and visually appealing, 
                    while preserving all original numbers and facts. Ensure that the output reflects a professional tone suitable
                    for internal discussions and communications within the company.
                    notes for the data you may be given:
                    > percentage - round to the second digit after the dot and add the percentage symbol - 2.22222 --> 2.22%.
                    > money - make the ILS currency symbol, round to the second digit after the dot - 1332.22222 --> 1,332.22₪.
                    > quantity - always show as an integer and round to 0 digits after the dot - 2.22222 --> 2.
                    > dates - format like this: dd/mm/yyyy - 2024-01-31 --> 31/01/2024.

                    finally: ensure that the your response is in the language used by your recieved input.
                    """
                    
                    decorator_response = model_reponse(answer, sys_decorator)
                    answer = decorator_response.choices[0].message.content.strip()
                    
                    decorator_usage_dict = decorator_response.to_dict()['usage']
                    error_usage_dict = {'completion_tokens': 0, 'prompt_tokens': 0, 'total_tokens': 0}

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
                error_response = model_reponse(prompt, sys_error)    
                answer = error_response.choices[0].message.content.strip()

                decorator_usage_dict = {'completion_tokens': 0, 'prompt_tokens': 0, 'total_tokens': 0}
                error_usage_dict = error_response.to_dict()['usage']
                 

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
            
            usage_dict['py_attempts_usage'] = py_attempts_usage_lst
            usage_dict['decorator_usage'] = decorator_usage_dict
            usage_dict['error_usage'] = error_usage_dict
            

            # memory control
            if len(st.session_state.base_history)>st.session_state.memory_limit:
                # delete the first 2 massages after system and dinamic samples
                # st.session_state.base_history = [st.session_state.base_history[0]]+st.session_state.base_history[3:]
                st.session_state.base_history = st.session_state.base_history[0:(1+st.session_state.n_most_similar*2)]+st.session_state.base_history[1+st.session_state.n_most_similar*2+2:]

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
            log_session.append(str(usage_dict))
            
            
        log_data.append(log_session)
        
        # Insert log data into SQL table  
        conn = pyodbc.connect(driver='{ODBC Driver 17 for SQL Server}',  
                              server='diplomat-analytics-server.database.windows.net',  
                              database='Diplochat-DB',  
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
                st.markdown(st.session_state.base_history)# for debug
                # st.markdown(usage_dict)
            
        # Feedback mechanism


        with st.form('form'):
            streamlit_feedback(feedback_type="thumbs",
                                optional_text_label="Enter your feedback here", 
                                align="flex-start", 
                                key='user_feedback')
            st.form_submit_button('Save feedback', on_click=handle_feedback)    

elif st.session_state['authentication_status'] is False:  
    st.error('Username/password is incorrect')  
elif st.session_state['authentication_status'] is None:  
    st.warning('Please enter your username and password')  

    
    


