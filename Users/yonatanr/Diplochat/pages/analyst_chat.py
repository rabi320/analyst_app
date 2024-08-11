import streamlit as st  
import pyodbc  
import pandas as pd  
from datetime import datetime  
from statsmodels.tsa.statespace.sarimax import SARIMAX
from openai import AzureOpenAI 
import warnings 
import time 
import re


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
   - **Note**: Filter the data for the date range between 2024-05-01 and 2024-05-31.  
  
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
     - `SALE_ID`: Identifier for a promotion.  
   - **Note**: Filter the category to snacks ('חטיפים') and the date range between 2024-05-01 and 2024-05-31 and the city to tel-aviv ('תל אביב').  

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
            WHERE Day BETWEEN '2024-05-01' AND '2024-05-31'
        \"\"\",
        'DW_DIM_STORENEXT_BY_INDUSTRIES_ITEMS': \"\"\"
            SELECT Barcode, Item_Name, Category_Name, Sub_Category_Name, Brand_Name, Sub_Brand_Name, Supplier_Name
            FROM [dbo].[DW_DIM_STORENEXT_BY_INDUSTRIES_ITEMS]
            WHERE Category_Name = N'חטיפים'
        \"\"\",
        'DW_CHP': \"\"\"
            SELECT ITEM_DESCRIPION, BARCODE, CHAIN_CODE, STORE_CODE, CHAIN, STORE, ADDRESS, CITY, SELLOUT_DESCRIPTION, STORENEXT_CATEGORY, SUPPLIER, FILE_DATE, PRICE, SELLOUT_PRICE, SALE_ID
            FROM [dbo].[DW_CHP]
            WHERE STORENEXT_CATEGORY = N'חטיפים' AND FILE_DATE BETWEEN '2024-05-01' AND '2024-05-31' AND CITY = N'תל אביב'
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

For any question you provide the answer in a python text variable named 'answer' after making the needed analysis.



Example:
> What is the highest selling item?

```python
# data analyzing and getting result...

item = 'פרינגלס'

answer = 'The highest selling item is [item]'
```

* A very important note on Predictive analytics! - when asked about a future event, make a forecast based on forcasting methods such as SARIMA to get the desired prediction (make sure to deactivate any printed output from the chosen model).

Context for the Questions of stakeholders:

>Market Cap (נתח שוק) - The Percent of total sales in NIS of a certain brand in his category - 
meaning if asked about a certain brand's market cap, then you need sum that brand's sales in the chosen time frame (Can be monthly, weekly or daily, group your dataframe accordingly), and devide it by the total sales in that brand's category,
you need to merge the stnx_sales and stnx_items dataframes to obtain all the neccesary data for that.

>textual data - all the textual data here is hebrew so take that in mind while filtering dataframes.

>Competitors (מתחרים) - When requeting data about competitors, we are the supplier name 'דיפלומט' in the data and other supliers in the same category are the competition. 

>Promotion Sales (מבצעים) - It is an actual promotion only where the 'SELLOUT_PRICE' in the chp dataset is bigger then 1. 
* Final note - make the 'answer' variable mimic an actual prompt generated by an LLM in the language you are addressed!
"""  
sys_error = """  
You are an assistant that informs the user when their input is unclear,  
and you ask them to provide more details or rephrase their message in the same language they used.  
"""  


examples = [{'role':'user','content':'מהם ניתחי השוק של מותג פרינגלס בפילוח שבועי'},{'role':'assistant','content':'To analyze the market cap of the Pringles brand on a weekly basis, we will first merge the `stnx_sales` and `stnx_items` dataframes to gather the necessary information, then calculate the weekly sales for Pringles and the total sales for the snacks category. Finally, we will compute the market cap.\n\nHere\'s the code to perform this analysis:\n\n```python\n# Merging sales data with items data\nmerged_data = stnx_sales.merge(stnx_items, on=\'Barcode\', how=\'inner\')\n\n# Filtering for Pringles brand\npringles_data = merged_data[merged_data[\'Brand_Name\'] == \'פרינגלס\']\n\n# Grouping by week and calculating total sales in NIS for Pringles\npringles_weekly_sales = pringles_data.resample(\'W-Mon\', on=\'Day\').agg({\'Sales_NIS\': \'sum\'}).reset_index()\n\n# Calculating total sales for the snacks category\ntotal_weekly_sales = merged_data.resample(\'W-Mon\', on=\'Day\').agg({\'Sales_NIS\': \'sum\'}).reset_index()\n\n# Merging to calculate market cap\nmarket_cap_data = pringles_weekly_sales.merge(total_weekly_sales, on=\'Day\', suffixes=(\'_Pringles\', \'_Total\'))\n\n# Calculating market cap percentage\nmarket_cap_data[\'Market_Cap_Percent\'] = (market_cap_data[\'Sales_NIS_Pringles\'] / market_cap_data[\'Sales_NIS_Total\']) * 100\n\n# Getting the weekly market cap values\nweekly_market_cap = market_cap_data[[\'Day\', \'Market_Cap_Percent\']]\n\nanswer = f"הנתח השוק של פרינגלס בשבועות הוא:\\n{weekly_market_cap.to_string(index=False)}"\n```\n\nThis code performs the following steps:\n1. Merges the sales and items dataframes to get item details along with sales figures.\n2. Filters the data for the Pringles brand.\n3. Groups the sales data by week and calculates total sales in NIS for Pringles.\n4. Computes total sales for the snacks category on a weekly basis.\n5. Merges the two dataframes to calculate the market cap percentage for Pringles.\n6. Prepares the final answer in a format suitable for presentation.\n\nYou can run this code in your Python environment to get the weekly market cap for Pringles.'},
{'role':'user','content':'מתי השבוע שנתח השוק של פרינגלס היה הכי נמוך מבין השבועות של מאי 24'},{'role':'assistant','content':'To determine when the market cap of the Pringles brand was the lowest among the weeks in May 2024, we can follow these steps:\n\n1. Merge the `stnx_sales` and `stnx_items` datasets to get the necessary details for Pringles.\n2. Filter the merged data for the date range of May 2024.\n3. Group the data by week and calculate the total sales for Pringles and the total sales for the snacks category.\n4. Calculate the market cap for each week.\n5. Identify the week with the lowest market cap.\n\nHere\'s the code to perform this analysis:\n\n```python\n# Merging sales data with items data\nmerged_data = stnx_sales.merge(stnx_items, on=\'Barcode\', how=\'inner\')\n\n# Filtering for Pringles brand\npringles_data = merged_data[merged_data[\'Brand_Name\'] == \'פרינגלס\']\n\n# Grouping by week and calculating total sales in NIS for Pringles\npringles_weekly_sales = pringles_data.resample(\'W-Mon\', on=\'Day\').agg({\'Sales_NIS\': \'sum\'}).reset_index()\n\n# Calculating total sales for the snacks category\ntotal_weekly_sales = merged_data.resample(\'W-Mon\', on=\'Day\').agg({\'Sales_NIS\': \'sum\'}).reset_index()\n\n# Merging to calculate market cap\nmarket_cap_data = pringles_weekly_sales.merge(total_weekly_sales, on=\'Day\', suffixes=(\'_Pringles\', \'_Total\'))\n\n# Calculating market cap percentage\nmarket_cap_data[\'Market_Cap_Percent\'] = (market_cap_data[\'Sales_NIS_Pringles\'] / market_cap_data[\'Sales_NIS_Total\']) * 100\n\n# Finding the week with the lowest market cap\nlowest_market_cap_week = market_cap_data.loc[market_cap_data[\'Market_Cap_Percent\'].idxmin()]\n\n# Formatting the answer\nanswer = f"השבוע שבו נתח השוק של פרינגלס היה הכי נמוך הוא {lowest_market_cap_week[\'Day\'].strftime(\'%d/%m/%Y\')} עם נתח שוק של {lowest_market_cap_week[\'Market_Cap_Percent\']:.2f}%."\n```\n\nIn this code:\n- We merge the sales data with item details to focus on Pringles.\n- We compute weekly sales and total sales for the snacks category.\n- We calculate the market cap percentage for each week.\n- Finally, we identify and format the week with the lowest market cap for the answer.'},
{'role':'user','content':'תתן לי 10 דוגמאות של תיאורי המבצעים ייחודיים של המתחרים באותה תקופה כרשימה'},{'role':'assistant','content':'Sure! Here are 10 examples of unique promotional descriptions from competitors during the specified time period:\n\n```python\n# First, we filter the \'chp\' dataframe to find promotions from competitors.\ncompetitor_promotions = chp[chp[\'SUPPLIER\'] != \'דיפלומט\']\n\n# Then, we filter out promotions where the SELLOUT_PRICE is greater than 1.\nunique_promotions = competitor_promotions[competitor_promotions[\'SELLOUT_PRICE\'] > 1]\n\n# Now, we extract unique promotion descriptions.\npromotion_descriptions = unique_promotions[\'SELLOUT_DESCRIPTION\'].unique()[:10]\n\n# Finally, we format the output.\nanswer = f"הנה 10 תיאורים ייחודיים של מבצעים של מתחרים במהלך התקופה: {\', \'.join(promotion_descriptions)}"\n```\n\nThis code snippet will provide the unique promotional descriptions of competitors in the specified period.'},
{'role':'user','content':'באיזה רשת יש את המבצע הנמוך ביותר בתקופה הזאת?'},{'role':'assistant','content':'To determine which supermarket chain has the lowest promotional price for snacks during the specified period in Tel Aviv, I\'ll analyze the `chp` dataset. I\'ll filter for promotions where the `SELLOUT_PRICE` is greater than 1 and then find the chain with the minimum promotional price. Here\'s the code to perform this analysis:\n\n```python\n# Filter for valid promotions\npromotions = chp[chp[\'SELLOUT_PRICE\'] > 1]\n\n# Find the chain with the lowest promotional price\nlowest_price_chain = promotions.loc[promotions[\'SELLOUT_PRICE\'].idxmin()]\n\nchain_name = lowest_price_chain[\'CHAIN\']\nlowest_price = lowest_price_chain[\'SELLOUT_PRICE\']\n\nanswer = f"הרשת עם המחיר הנמוך ביותר עבור חטיפים היא {chain_name} במחיר של {lowest_price} ש"ח."\n```\n\nThis code snippet filters the promotional data, finds the minimum promotional price, and constructs an answer string that summarizes the findings in Hebrew.'}]


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
        'DW_CHP': """
            SELECT ITEM_DESCRIPION, BARCODE, CHAIN_CODE, STORE_CODE, CHAIN, STORE, ADDRESS, CITY, SELLOUT_DESCRIPTION, STORENEXT_CATEGORY, SUPPLIER, FILE_DATE, PRICE, SELLOUT_PRICE, SALE_ID
            FROM [dbo].[DW_CHP]
            WHERE STORENEXT_CATEGORY = N'חטיפים' AND FILE_DATE BETWEEN '2024-05-01' AND '2024-05-31' AND CITY = N'תל אביב'
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
    


    st.title("Diplochat AI 🤖")  

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

    if "openai_model" not in st.session_state:  
        st.session_state["openai_model"] = MODEL  
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
    
    if prompt := st.chat_input("Ask me anything"):
        st.session_state.messages.append({"role": "user", "content": prompt})
        base_history.append({"role": "user", "content": prompt})
        with st.chat_message("user", avatar=user_avatar):
            st.markdown(prompt)

        
        with st.spinner("Thinking..."):
            
            answer = ''
            txt = ''
            
            max_attempts = 5
            errors = []
            attempts = 0
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
                    
                    

                    with st.chat_message("assistant", avatar='🤖'):
                        # Create a placeholder for streaming output  
                        placeholder = st.empty()  
                        streamed_text = ""  
                        
                        # Stream the answer output  
                        for char in answer:  
                            streamed_text += char  
                            placeholder.markdown(streamed_text)  
                            time.sleep(0.01)  # Adjust the sleep time to control the streaming speed 
                    base_history.append({"role": "assistant", "content": answer})
                    break

                except Exception as e:  
                    errors.append(f"Attempt {attempts + 1} failed: {e}")  
                    attempts += 1
                    
                                # generate anwer for failures
            if attempts == max_attempts:
                # replace with ai generated text
                # answer = 'לא מצאתי תשובה, נסה לנסח מחדש בבקשה'
                answer = generate_text(prompt, sys_error)
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
                
                

                          
                        