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

# Suppress all warnings  
warnings.filterwarnings('ignore')   
  
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
   - **Note**: Filter the data for the date range between 2024-03-01 and 2024-05-31.  
  
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
   - **Note**: Filter the category to snacks ('חטיפים').  
  
3. **DW_CHP** (`chp`)  
   - **Description**: This fact table records daily snack prices by barcode and store, including promotions.  
   - **Columns**:  
     - `ITEM_DESCRIPION`: Name of the item.  
     - `BARCODE`: Item identifier.  
     - `CHAIN_CODE`: Supermarket chain code.  
     - `STORE_CODE`: Store code.  
     - `CHAIN`: Name of the supermarket chain.  
     - `STORE`: Name of the store.  
     - `ADDRESS`: Street and number.  
     - `CITY`: Name of the city.  
     - `SELLOUT_DESCRIPTION`: Hebrew description of sales promotions.  
     - `STORENEXT_CATEGORY`: Category name.  
     - `SUPPLIER`: Supplier name.  
     - `FILE_DATE`: Date (datetime).  
     - `PRICE`: Base price.  
     - `SELLOUT_PRICE`: Promotional price.  
   - **Note**: Filter the category to snacks ('חטיפים') and the date range between 2024-03-01 and 2024-05-31 and only Sundays. and non-null sellout prices.  

this is the code that already loaded the data to the IDE:

```python
def load_data():  
    conn = pyodbc.connect(driver='{ODBC Driver 17 for SQL Server}',  
                          server='diplomat-analytics-server.database.windows.net',  
                          database='NBO-DB',  
                          uid='analyticsadmin', pwd='Analytics12345')  
  

    #Define tables and queries
    tables = {
        'DW_FACT_STORENEXT_BY_INDUSTRIES_SALES': \"\"\"
            SELECT Day, Barcode, Format_Name, Sales_NIS, Sales_Units, Price_Per_Unit
            FROM [dbo].[DW_FACT_STORENEXT_BY_INDUSTRIES_SALES]
            WHERE Day BETWEEN '2024-03-01' AND '2024-05-31'
        \"\"\",
        'DW_DIM_STORENEXT_BY_INDUSTRIES_ITEMS': \"\"\"
            SELECT Barcode, Item_Name, Category_Name, Sub_Category_Name, Brand_Name, Sub_Brand_Name, Supplier_Name
            FROM [dbo].[DW_DIM_STORENEXT_BY_INDUSTRIES_ITEMS]
            WHERE Category_Name = N'חטיפים'
        \"\"\",
        'DW_CHP': \"\"\"
            SELECT ITEM_DESCRIPION, BARCODE, CHAIN_CODE, STORE_CODE, CHAIN, STORE, ADDRESS, CITY, SELLOUT_DESCRIPTION, STORENEXT_CATEGORY, SUPPLIER, FILE_DATE, PRICE, SELLOUT_PRICE
            FROM [dbo].[DW_CHP]
            WHERE STORENEXT_CATEGORY = N'חטיפים' AND FILE_DATE BETWEEN '2024-03-01' AND '2024-05-31' AND SELLOUT_PRICE IS NOT NULL AND DATENAME(WEEKDAY, FILE_DATE) = 'Sunday'
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
chp = dataframes['DW_CHP']

# Convert date columns to datetime
stnx_sales['Day'] = pd.to_datetime(stnx_sales['Day'])
chp['FILE_DATE'] = pd.to_datetime(chp['FILE_DATE'])

```

Quesstions Convention - 

For any question you provide a code in python and in the end give the the answer in a python text variable named 'answer' after making the needed analysis.

* A very important note on Predictive analytics! - when asked about a future event, make a forecast based on forcasting methods such as SARIMA to get the desired prediction (make sure to deactivate any printed output from the chosen model).

Context for the Questions of stakeholders:

>Market Cap (נתח שוק) - The Percent of total sales in NIS of a certain brand in his category - 
meaning if asked about a certain brand's market cap, then you need sum that brand's sales in the chosen time frame (Can be monthly, weekly or daily, group your dataframe accordingly), and devide it by the total sales in that brand's category,
you need to merge the stnx_sales and stnx_items dataframes to obtain all the neccesary data for that.

>textual data - all the textual data here is hebrew so take that in mind while filtering dataframes.

>Competitors (מתחרים) - When requeting data about competitors, we are the supplier name 'דיפלומט' in the data and other supliers in the same category are the competition. 

>Promotion Sales (מבצעים) - It is an actual promotion only where the 'SELLOUT_PRICE' in the chp dataset is bigger then 1. 
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
  'content': 'תתן לי 10 דוגמאות של תיאורי המבצעים ייחודיים של המתחרים באותה תקופה כרשימה'},
 {'role': 'assistant',
  'content': '```python\n# First, we filter the \'chp\' dataframe to find promotions from competitors.\ncompetitor_promotions = chp[chp[\'SUPPLIER\'] != \'דיפלומט\']\n\n# Then, we filter out promotions where the SELLOUT_PRICE is greater than 1.\nunique_promotions = competitor_promotions[competitor_promotions[\'SELLOUT_PRICE\'] > 1]\n\n# Now, we extract unique promotion descriptions.\npromotion_descriptions = unique_promotions[\'SELLOUT_DESCRIPTION\'].unique()[:10]\n\n# Finally, we format the output.\nanswer = f"הנה 10 תיאורים ייחודיים של מבצעים של מתחרים במהלך התקופה: {\', \'.join(promotion_descriptions)}"\n```'},
 {'role': 'user', 'content': 'באיזה רשת יש את המבצע הנמוך ביותר בתקופה הזאת?'},
 {'role': 'assistant',
  'content': '```python\n# Filter for valid promotions\npromotions = chp[chp[\'SELLOUT_PRICE\'] > 1]\n\n# Find the chain with the lowest promotional price\nlowest_price_chain = promotions.loc[promotions[\'SELLOUT_PRICE\'].idxmin()]\n\nchain_name = lowest_price_chain[\'CHAIN\']\nlowest_price = lowest_price_chain[\'SELLOUT_PRICE\']\n\nanswer = f"הרשת עם המחיר הנמוך ביותר עבור חטיפים היא {chain_name} במחיר של {lowest_price} ש"ח."\n```'},
 {'role': 'user',
  'content': 'מה היה המחיר הממוצע של פרינגלס בשבוע עם נתח השוק הגבוה ביותר'},
 {'role': 'assistant',
  'content': '```python\nimport pandas as pd\n\n# Filter for Pringles items\npringles_items = stnx_items[stnx_items[\'Brand_Name\'] == \'פרינגלס\']\n\n# Merge sales and items dataframes\nmerged_sales = pd.merge(stnx_sales, pringles_items[[\'Barcode\', \'Brand_Name\']], on=\'Barcode\')\n\n# Group by week and calculate total sales in NIS for Pringles and total sales for the category\nmerged_sales[\'Week\'] = merged_sales[\'Day\'].dt.isocalendar().week\nweekly_sales = merged_sales.groupby([\'Week\']).agg({\'Sales_NIS\': \'sum\'}).reset_index()\n\n# Calculate total sales for the snacks category for each week\ntotal_weekly_sales = stnx_sales.groupby(stnx_sales[\'Day\'].dt.isocalendar().week).agg({\'Sales_NIS\': \'sum\'}).reset_index()\ntotal_weekly_sales.columns = [\'Week\', \'Total_Sales_NIS\']\n\n# Merge to calculate market share\nweekly_sales = pd.merge(weekly_sales, total_weekly_sales, on=\'Week\', suffixes=(\'_Pringles\', \'_Total\'))\nweekly_sales[\'Market_Share\'] = weekly_sales[\'Sales_NIS_Pringle\'] / weekly_sales[\'Total_Sales_NIS\']\n\n# Find the week with the highest market share for Pringles\nmax_market_share_week = weekly_sales.loc[weekly_sales[\'Market_Share\'].idxmax(), \'Week\']\n\n# Filter sales data for Pringles during that week\npringles_week_data = merged_sales[merged_sales[\'Week\'] == max_market_share_week]\n\n# Calculate the average price per unit for Pringles during that week\naverage_price = pringles_week_data[\'Price_Per_Unit\'].mean()\n\nanswer = f\'הממוצע של המחיר של פרינגלס בשבוע עם נתח השוק הגבוה ביותר הוא {average_price:.2f} ש"ח.\'\n```'}]


client = AzureOpenAI(  
    azure_endpoint="https://ai-usa.openai.azure.com/",  
    api_key='86bedc710e5e493290cb2b0ce6f16d80',  
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
            Total_Time FLOAT  
        )  
    END  
    """  
      
    cursor = conn.cursor()  
    cursor.execute(create_table_query)  
      
    # Insert log data into the AI_LOG table  
    insert_query = """  
    INSERT INTO AI_LOG (Conversation_ID, Timestamp, User_Name, User_Prompt, LLM_Responses, Code_Extractions, Final_Answer, Num_Attempts, Num_LLM_Calls, Errors, Total_Time)  
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)  
    """  
      
    cursor.execute(insert_query, log_session)  
    conn.commit()  
    cursor.close()

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
            WHERE Day BETWEEN '2024-03-01' AND '2024-05-31' AND BARCODE IN (SELECT T.BARCODE FROM (SELECT [Barcode]
            ,[Item_Name]
            ,[Category_Name]
            FROM [dbo].[DW_DIM_STORENEXT_BY_INDUSTRIES_ITEMS]
            WHERE Category_Name = N'חטיפים') T)
        """,
        'DW_DIM_STORENEXT_BY_INDUSTRIES_ITEMS': """
            SELECT Barcode, Item_Name, Category_Name, Sub_Category_Name, Brand_Name, Sub_Brand_Name, Supplier_Name
            FROM [dbo].[DW_DIM_STORENEXT_BY_INDUSTRIES_ITEMS]
            WHERE Category_Name = N'חטיפים'
        """,
        'DW_CHP': """
            SELECT ITEM_DESCRIPION, BARCODE, CHAIN_CODE, STORE_CODE, CHAIN, STORE, ADDRESS, CITY, SELLOUT_DESCRIPTION, STORENEXT_CATEGORY, SUPPLIER, FILE_DATE, PRICE, SELLOUT_PRICE
            FROM [dbo].[DW_CHP]
            WHERE STORENEXT_CATEGORY = N'חטיפים' AND FILE_DATE BETWEEN '2024-03-01' AND '2024-05-31' AND SELLOUT_PRICE IS NOT NULL AND DATENAME(WEEKDAY, FILE_DATE) = 'Sunday'
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
    

    user_name = st.session_state.get("name", "Guest")  # Default to "Guest" if not set

    st.title(f"Diplochat AI Analyst 🤖 - {user_name}")  

    dataframes = load_data()  
    
    # Assigning dataframes to variables
    stnx_sales = dataframes['DW_FACT_STORENEXT_BY_INDUSTRIES_SALES']
    stnx_items = dataframes['DW_DIM_STORENEXT_BY_INDUSTRIES_ITEMS']
    chp = dataframes['DW_CHP']

    # Convert date columns to datetime
    stnx_sales['Day'] = pd.to_datetime(stnx_sales['Day'])
    chp['FILE_DATE'] = pd.to_datetime(chp['FILE_DATE'])

    user_avatar = '🧑'

    client = AzureOpenAI(  
        azure_endpoint="https://ai-usa.openai.azure.com/",  
        api_key='86bedc710e5e493290cb2b0ce6f16d80',  
        api_version="2024-02-15-preview"  
    )  
    MODEL = "Diplochat"  
    
    base_history = [{"role": "system", "content": sys_msg}]+examples

    # Initialize log_dfs 
    if 'log_dfs' not in st.session_state:  
        st.session_state.log_dfs = []  
    
    if "openai_model" not in st.session_state:  
        st.session_state["openai_model"] = MODEL 
    
    if 'user_feedback' not in st.session_state:  
        st.session_state.user_feedback = 'not rated' 

    if 'feedback' not in st.session_state:
        st.session_state.feedback = []

    if "messages" not in st.session_state:  
        st.session_state.messages = [{"role": "system", "content": sys_msg}]
        
    answer = ''
    # Display chat messages from history on app rerun  
    for message in st.session_state.messages:  
        if message["role"] == 'assistant':  
            with st.chat_message(message["role"], avatar='🤖'):  
                st.markdown(message["content"])

        elif message["role"] == 'user':  
            with st.chat_message(message["role"], avatar=user_avatar):  
                st.markdown(message["content"])  
    
    log_data = []
    # data in each session: prompt,txt_content,code_lst,
    log_session = []

    log_cols = ['Conversation_ID','Timestamp','User_Name','User_Prompt','LLM_Responses','Code_Extractions','Final_Answer','Num_Attempts','Num_LLM_Calls','Errors','Total_Time']
    log_dfs = []

    israel_tz = pytz.timezone("Asia/Jerusalem")
    
    now = datetime.now(israel_tz).strftime("%Y-%m-%d %H:%M:%S") 
    name_id = ''.join(i for i in user_name if i==i.upper()).replace(' ','')
    ts_id = ''.join(i for i in now if i.isdigit())

    conv_id = f'{name_id}_{ts_id}'
    # Initialize the conversation ID in session state if it doesn't exist  
    if 'conv_id' not in st.session_state:  
        st.session_state.conv_id = f'{name_id}_{ts_id}'



    if prompt := st.chat_input("Ask me anything"):
        prompt_timestamp = datetime.now(israel_tz).strftime("%Y-%m-%d %H:%M:%S") 
        st.session_state.messages.append({"role": "user", "content": prompt})
        base_history.append({"role": "user", "content": prompt})
        with st.chat_message("user", avatar=user_avatar):
            st.markdown(prompt)

            start_time = time.time()
        

        with st.spinner("Thinking..."):
            
            answer = ''
            txt = ''
            
            max_attempts = 10
            errors = []
            attempts = 0
            txt_content_lst = []
            code_lst = []
            n_llm_api_call = 0


            while attempts < max_attempts:
                
                try:

                    txt = client.chat.completions.create(
                        model=st.session_state["openai_model"],
                        messages=[
                            {"role": m["role"], "content": m["content"]}
                            for m in base_history
                        ],
                        max_tokens=2000,
                        stream=False,
                    )
                    txt_content = txt.choices[0].message.content
                    
                    n_llm_api_call+=1
                    # append original gen ai content to the list
                    txt_content_lst.append(txt_content)

        
                    # st.text(txt_content)
                
                    # Regex pattern to extract the Python code
                    pattern = r'```python(.*?)```'   
                    all_code = re.findall(pattern, txt_content, re.DOTALL)
                    if len(all_code) == 1:  
                        code = all_code[0]
                        
                    else:  
                        code = '\n'.join(all_code)              
                    
                    
                    # st.text(code)
                    # st.text(type(code))
                    
                    # code = extract_code(txt_content)
                    
                    # Use re.sub to comment out any print statement  
                    code = re.sub(r"^(\s*)print\(", r"\1#print(", code, flags=re.MULTILINE)
                    # Use re.sub to comment out any import statement  
                    code = re.sub(r"^(\s*)import\s", r"\1#import ", code, flags=re.MULTILINE)  

                    # st.text(code)
                    


                    
                    local_context = {'chp':chp,'stnx_sales':stnx_sales,'stnx_items':stnx_items,'pd':pd,'SARIMAX':SARIMAX}
                    exec(code, {}, local_context)
                    answer = local_context.get('answer', "No answer found.") 
                    
                    if answer == "No answer found.":  
                        raise ValueError("No answer found.")  
                    
                    sys_decorator = """
                    You are an AI assistant created to enhance the aesthetic quality of LLM responses. 
                    Your task is to take the generated response and find ways to make it more articulate and visually appealing, 
                    while preserving the original numbers and facts, ensuring that the output resembles that of a language model.
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

                    base_history.append({"role": "assistant", "content": history_msg})
                    
                    # append regex formatted code with python markdown content to the list
                    code_lst.append(history_msg)
                    attempts += 1
                    break

                except Exception as e:  
                    errors.append(f"Attempt {attempts + 1} failed: {e}")  
                    attempts += 1
                    
                                # generate anwer for failures
            if attempts == max_attempts:
                # replace with ai generated text
                # answer = 'לא מצאתי תשובה, נסה לנסח מחדש בבקשה'
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
                base_history = base_history[:-1]
            
            st.session_state.messages.append({"role": "assistant", "content": answer})

            elapsed_time = time.time() - start_time
            
            

            # append rest of the data from the session
            
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
        log_data.append(log_session)
        
        # Insert log data into SQL table  
        conn = pyodbc.connect(driver='{ODBC Driver 17 for SQL Server}',  
                              server='diplomat-analytics-server.database.windows.net',  
                              database='NBO-DB',  
                              uid='analyticsadmin', pwd='Analytics12345')  
        insert_log_data(conn, log_session)  
        conn.close()  
        
        tmp_df = pd.DataFrame(log_data,columns=log_cols)
        # log_dfs.append(tmp_df)

        st.session_state.log_dfs.append(tmp_df)
        
        
        # log_df = pd.concat(log_dfs,axis=0).reset_index(drop = True)
        log_df = pd.concat(st.session_state.log_dfs, axis=0).reset_index(drop=True)
        
        # st.table(log_df)
        # if len(log_df)>1:
        #     log_df.loc[len(log_df)-2, 'Final_Answer'] = f'{st.session_state.user_feedback}'

        # Create an expander  
        with st.expander("Show Log DataFrame"):  
            # Your code inside the expander  
            st.dataframe(log_df)
        
        # Feedback mechanism

        # st.write("### Was this response helpful?")
        # feedback_options = ["Very Helpful", "Somewhat Helpful", "Not Helpful", "Irrelevant"]
        # user_feedback = st.radio("Please select your feedback:", feedback_options)

        # # Save the feedback in session state
        # if user_feedback:
        #     st.session_state.feedback.append({
        #         "timestamp": prompt_timestamp,
        #         "prompt": prompt,
        #         "answer": answer,
        #         "feedback": user_feedback
        #     })
        #     st.success("Thank you for your feedback!")

        # # Display the feedback history if needed
        # st.write("### Feedback History")
        # st.write(st.session_state.feedback)
        
        # with st.expander("Rate me!"): 
        #     feedback = streamlit_feedback(feedback_type="thumbs")
        #     st.markdown(str(feedback))
        #     # selected = st.feedback("stars")
        #     # if selected is not None:
        #     #     st.session_state.user_feedback = selected
        

        
        

        #     if 'clicked' not in st.session_state:
        #         st.session_state.clicked = False

        #     def click_button():
        #         st.session_state.clicked = True

        #     st.button('Rate me', on_click=click_button)

        #     if st.session_state.clicked:
        #         # The message and nested widget will remain on the page
        #         st.write('Rate ⭐')
        #         st.session_state.user_feedback = st.slider('Select a value',value = (1, 5))

            

            
            
                         
    # if prompt := st.chat_input("Ask me anything"): 
    #     response_text = "" 
    #     st.session_state.messages.append({"role": "user", "content": prompt})  
    #     with st.chat_message("user", avatar=user_avatar):  
    #         st.markdown(prompt)  
    #     with st.chat_message("assistant", avatar='🤖'):  
    #         max_attempts = 5  
    #         errors = []  
    #         attempts = 0  
    #         answer = ''  
    #         response_placeholder = st.empty()  
    #         response_text = ""  
    #         with st.spinner("Thinking..."):
    #             while attempts < max_attempts:  
    #                 try:  
    #                     txt = generate_text(prompt, sys_msg, st.session_state.messages)  
    #                     code = extract_code(txt)  
    #                     code = comment_out_lines(code, print_drop=True, data_drop=True)
    #                     local_context = {'chp':chp,'stnx_sales':stnx_sales,'stnx_items':stnx_items,'pd':pd,'SARIMAX':SARIMAX}
    #                     exec(code, {}, local_context)
    #                     # st.text(code)
    #                     # answer = local_context.get('answer', "No answer found.")    
    #                     # Simulate streaming by breaking response into smaller parts  
    #                     for i in range(0, len(code), 10):  # Adjust the chunk size as needed  
    #                         chunk = code[i:i+10]  
    #                         response_text += chunk  
    #                         response_placeholder.markdown(response_text)  
    #                         time.sleep(0.1)  # Adjust delay as needed  
    #                     st.session_state.messages.append({'role': 'assistant', 'content': code})
    #                     break  
    #                 except Exception as e:  
    #                     errors.append(f"Attempt {attempts + 1} failed: {e}")  
    #                     attempts += 1  
    #                     answer = errors[-1]

    
    #             if attempts == max_attempts:  
                    
    #                 answer = generate_text(sys_error, prompt, st.session_state.messages)  
    #                 # answer = errors[-1]
    #                 # Simulate streaming for the final response  
    #                 response_placeholder = st.empty()  
    #                 response_text = ""  
    #                 for i in range(0, len(answer), 10):  # Adjust the chunk size as needed  
    #                     chunk = answer[i:i+10]  
    #                     response_text += chunk  
    #                     response_placeholder.markdown(response_text)  
    #                     time.sleep(0.1)  # Adjust delay as needed
                
                

                          
                        