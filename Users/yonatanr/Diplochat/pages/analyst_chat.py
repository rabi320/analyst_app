import streamlit as st  
from openai import AzureOpenAI  
import pandas as pd  
import re  
import time  
import warnings    
import sys  
import io

# Suppress all warnings  
warnings.filterwarnings('ignore')  
  
sys_msg = """  
You are an AI Data Analyst assistant For DIPLOMAT DISTRIBUTORS (1968) LTD,
You are coding in python.
You have 3 datasets in your database:
1) DW_FACT_STORENEXT_BY_INDUSTRIES_SALES
    brief explanation - Sales by items in a daily level by different market segmentations.
    
    columns: 
        >Day - Date(datetime).
        >Barcode - Item.
        >Format_Name - Market segementation.
        >Sales_NIS - Sales in NIS.
        >Sales_Units - Sales Quantity.
        >Price_Per_Unit - Price Per Unit Daily.
        
        rest of the columns: Sales_(Gr),Sales_(Ml),UPDATE_DATE hold no significance, ignore them.
    note - need to filter the date between 2024-03-01 and 2024-05-31 



2) DW_DIM_STORENEXT_BY_INDUSTRIES_ITEMS
    brief explanation - Dimention table of all attributes regarding items.

    columns: 
        >Barcode - Item.
        >Item_Name - Item's name.
        >Category_Name - Category's name.
        >Sub_Category_Name - Sub Category's name.
        >Brand_Name - Brand's name.
        >Sub_Brand_Name - Sub Brand's name.
        >Supplier_Name - Supplier's name.

        rest of the columns: FMCG_Name,Weight Volume,core,core_food,core_PG,Unit of Measure	Parallel_IND,Class_Name,UPDATE_DATE hold no significance, ignore them.        
    note - need to filter the category on snacks - ('חטיפים')

2) DW_CHP
    brief explanation - fact table of all snack price daily reports per barcode and store, including promotions (e.g. 1 unit of pringles in a certain store at a certain day costs 3 ILS).

    columns: 
        >ITEM_DESCRIPION - Item's name.
        >BARCODE - Item.   
        >CHAIN_CODE	- Supermarket chain's code.
        >STORE_CODE - Store's code.
        >CHAIN - Supermarket chain's name.
        >STORE - Store's name.
        >ADDRESS - Street and number information.
        >CITY - City's name
        >SELLOUT_DESCRIPTION - Hebrew description of the sales promotions.
        >STORENEXT_CATEGORY - Category's name.
        >SUPPLIER - Supplier's name.
        >FILE_DATE - Date(datetime)
        >PRICE - Base price.
        >SELLOUT_PRICE - Promotion price.
        >SALE_ID - An identifier for a promotion.

        rest of the columns: TARGET_AUDIENCE,UP_TO,LIMITATIONS,NUMBER_OF_SALE_UNITS,DISCOUNT_RATE hold no significance, ignore them.
    note - need to filter the category on snacks - ('חטיפים') and the date between 2024-03-01 and 2024-05-31 

Data Extraction and naming convention:
    -Database: 'NBO-DB'
    -loading:
    this is how you load the data:
    ```python
    def load_data():
    # Connect to SQL Server
    conn = pyodbc.connect(driver='{ODBC Driver 17 for SQL Server}',
                          server='diplomat-analytics-server.database.windows.net',
                          database='NBO-DB',
                          uid='analyticsadmin', pwd='Analytics12345')
    # list of all the 3 tables:
    tbl_lst = [t1,t2,t2]# replace t1-3 with our defined 3 data tables 
    tbl_dict = {}
    for tbl in tbl_lst:
        # filter on snacks with a WHERE cluase (make sure to use the N'' because the text is hebrew)
        query = 'SELECT * FROM [dbo].[{tbl}] WHERE ...'

        # Execute the query and read the results into a DataFrame  
        df = pd.read_sql_query(query, conn)
        tbl_dict[tbl] = df  
    conn.close()
    # save by naming convention
    df_name1 = tbl_dict[t1] 
    df_name2 = tbl_dict[t2] 
    df_name3 = tbl_dict[t3] 
    
    
    return 

    -Naming convention: 
        >save all tables in this variables names as pandas Dataframes- 
            >DW_FACT_STORENEXT_BY_INDUSTRIES_SALES - stnx_sales
            >DW_DIM_STORENEXT_BY_INDUSTRIES_ITEMS - stnx_items
            >DW_CHP - chp

    * Data Note - Make sure all date values are as date and not object after saving them as dataframes
            

Quesstions Convention - 
For any question you provide the answer in a python text variable named 'answer' after making the needed analysis.

Example:
> What is the highest selling item?

```python
# data analyzing and getting result...

item = 'פרינגלס'

answer = 'The highest selling item is [item]'
```

Context for the Questions of stakeholders:
>Market Cap (נתח שוק) - The Percent of total sales in NIS of a certain brand in his category - 
meaning if asked about a certain brand's market cap, then you need sum that brand's sales in the chosen time frame, and devide it by the total sales in that brand's category,
you need to merge the stnx_sales and stnx_items dataframes to obtain all the neccesary data for that.
>textual data - all the textual data here is hebrew so take that in mind while filtering dataframes.
>Competitors (מתחרים) - When requeting data about competitors, we are the supplier name 'דיפלומט' in the data and other supliers in the same category are the competition. 
>Promotion Sales - It is an actual promotion only where the 'SELLOUT_PRICE' in the chp dataset is bigger then 1. 
>Predictive analytics - when asked about a future event, make a forecast based on forcasting methods such as SARIMA to get the desired prediction (make sure to deactivate any printed output from the chosen model).
* Final note - make the 'answer' variable mimic an actual prompt generated by an LLM!
"""  
  
client = AzureOpenAI(  
    azure_endpoint="https://ai-usa.openai.azure.com/",  
    api_key='86bedc710e5e493290cb2b0ce6f16d80',  
    api_version="2024-02-15-preview"  
)  
MODEL = "GPT_O"  
  
def generate_text(prompt, sys_msg, examples=[]):  
    response = client.chat.completions.create(  
        model="GPT_O",  # model = "deployment_name"  
        messages=[{"role": "system", "content": sys_msg}] + examples + [{"role": "user", "content": prompt}],  
        temperature=0.7,  
        max_tokens=2000,  
        top_p=0.95,  
        frequency_penalty=0,  
        presence_penalty=0,  
        stop=None  
    )  
    return response.choices[0].message.content.strip()  
  
def extract_code(txt):  
    pattern = r'python(.*?)'  
    all_code = re.findall(pattern, txt, re.DOTALL)  
    if len(all_code) == 1:  
        final_code = all_code[0]  
    else:  
        final_code = '\n'.join(all_code)  
    return final_code  
  
def comment_out_lines(code, print_drop=False, data_drop=True):  
    # Define the patterns and replacements  
    lines_to_comment = ["stnx_sales, stnx_items, chp = load_data()"]  
    if data_drop:  
        # Comment out the specific lines  
        for line in lines_to_comment:  
            pattern = re.compile(r"^(\s*)" + line, re.MULTILINE)  
            code = pattern.sub(r"\1# " + line, code)  
    if print_drop:  
        # Replace any print() statements with #print()  
        code = re.sub(r"^(\s*)print\(", r"\1#print(", code, flags=re.MULTILINE)  
    return code  

answer = ''
def run_code_in_subprocess(code, queue):  
    try:  
        # Execute the code  
        exec(code, globals())  
        # Put the answer in the queue  
        queue.put(answer)  
    except Exception as e:  
        queue.put(str(e)) 


def run():  
    st.title("Diplomat's AI Analyst 🤖")  
    st.markdown("![](https://www.globrands.co.il/main/wp-content/uploads/2024/01/diplomat_logo.png)")
    uploaded_file = st.file_uploader("Choose an Avatar for yourself: 📷🧑", type=["jpg", "jpeg", "png"])  
    if uploaded_file is not None:  
        image = uploaded_file.read()  
        user_avatar = image  
    else:  
        user_avatar = '🧑'  
  
    client = AzureOpenAI(  
        azure_endpoint="https://ai-usa.openai.azure.com/",  
        api_key='86bedc710e5e493290cb2b0ce6f16d80',  
        api_version="2024-02-15-preview"  
    )  
    MODEL = "Diplochat"  
  
    if "openai_model" not in st.session_state:  
        st.session_state["openai_model"] = MODEL  
    if "messages" not in st.session_state:  
        st.session_state.messages = [{"role": "system", "content": sys_msg}]  
  
    # Display chat messages from history on app rerun  
    for message in st.session_state.messages:  
        if message["role"] == 'assistant':  
            with st.chat_message(message["role"], avatar='🤖'):  
                st.markdown(message["content"])  
        elif message["role"] == 'user':  
            with st.chat_message(message["role"], avatar=user_avatar):  
                st.markdown(message["content"])  
  
    if prompt := st.chat_input("Ask me anything"): 
        response_text = "" 
        st.session_state.messages.append({"role": "user", "content": prompt})  
        with st.chat_message("user", avatar=user_avatar):  
            st.markdown(prompt)  
        with st.chat_message("assistant", avatar='🤖'):  
            max_attempts = 5  
            errors = []  
            attempts = 0  
            answer = ''  
            response_placeholder = st.empty()  
            response_text = ""  
            with st.spinner("Thinking..."):
                while attempts < max_attempts:  
                    try:  
                        txt = generate_text(prompt, sys_msg, st.session_state.messages)  
                        code = extract_code(txt)  
                        if attempts == 0:  
                            code = comment_out_lines(code, print_drop=True, data_drop=False)  
                        else:  
                            code = comment_out_lines(code, print_drop=True, data_drop=True)  
                        
  
                        # Redirect stdout to capture print statements  
                        old_stdout = sys.stdout  
                        redirected_output = sys.stdout = io.StringIO()  
  
                        # Execute the code in a controlled environment  
                        local_vars = {}  
                        exec(code, globals(), local_vars)  
                        
                        sys_error = """  
                        You are an assistant that informs the user when their input is unclear,  
                        and you ask them to provide more details or rephrase their message in the same language they used.  
                        """  
                        
                        # Capture the answer variable  
                        answer = local_vars.get('answer', generate_text(sys_error, prompt, st.session_state.messages))  
  
                        # Reset stdout  
                        sys.stdout = old_stdout  
  
  
    
                        # Simulate streaming by breaking response into smaller parts  
                        for i in range(0, len(answer), 10):  # Adjust the chunk size as needed  
                            chunk = answer[i:i+10]  
                            response_text += chunk  
                            response_placeholder.markdown(response_text)  
                            time.sleep(0.1)  # Adjust delay as needed  
                        st.session_state.messages.append({'role': 'assistant', 'content': answer})
                        break  
                    except Exception as e:  
                        errors.append(f"Attempt {attempts + 1} failed: {e}")  
                        attempts += 1  
                        answer = errors[-1]
                    
                     

    
                # if attempts == max_attempts:  
                #     # answer = generate_text(sys_error, prompt, st.session_state.messages)  
                #     answer = errors[-1]
                #     # Simulate streaming for the final response  
                #     response_placeholder = st.empty()  
                #     response_text = ""  
                #     for i in range(0, len(answer), 10):  # Adjust the chunk size as needed  
                #         chunk = answer[i:i+10]  
                #         response_text += chunk  
                #         response_placeholder.markdown(response_text)  
                #         time.sleep(0.1)  # Adjust delay as needed
                