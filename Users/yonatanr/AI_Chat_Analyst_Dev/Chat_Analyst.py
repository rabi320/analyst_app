import pandas as pd
from openai import AzureOpenAI
import re
import time
import warnings  
  
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
  azure_endpoint = "https://ai-usa.openai.azure.com/", 
  api_key='86bedc710e5e493290cb2b0ce6f16d80',  
  api_version="2024-02-15-preview"
)
MODEL="GPT_O"





def generate_text(prompt, sys_msg,examples = []):  
    response = client.chat.completions.create(
    model="GPT_O", # model = "deployment_name"
    messages = [{"role":"system","content":sys_msg}]+examples+[{"role":"user","content":prompt}],
    temperature=0.7,
    max_tokens=2000,
    top_p=0.95,
    frequency_penalty=0,
    presence_penalty=0,
    stop=None)
    return response.choices[0].message.content.strip()  


def extract_code(txt):
    pattern = r'```python(.*?)```'
    all_code = re.findall(pattern, txt, re.DOTALL)
    if len(all_code)==1:
        final_code = all_code[0]
    else:
        final_code = '\n'.join(all_code)
    return final_code

# Function to stream the text character by character  
def stream_text(text, delay=0.01):  
    initial_message = 'Assistant 🤖: '  
    print(initial_message, end='', flush=True)  
    time.sleep(2)
    for char in text:  
        print(char, end='', flush=True)  
        time.sleep(delay)  
    print()  # For final newline

def comment_out_lines(code,print_drop = False,data_drop = True):  
    
    
    # Define the patterns and replacements  
    lines_to_comment = [  
        "stnx_sales, stnx_items, chp = load_data()"
    ]  

    if data_drop:  
        # Comment out the specific lines  
        for line in lines_to_comment:  
            pattern = re.compile(r"^(\s*)" + line, re.MULTILINE)  
            code = pattern.sub(r"\1# " + line, code)  
        
    if print_drop:
    # Replace any print() statements with #print()  
        code = re.sub(r"^(\s*)print\(", r"\1#print(", code, flags=re.MULTILINE)  
      
    return code 


# Suppress all warnings  
warnings.filterwarnings('ignore')  

conversation_history = []

conv_cnt = 0
while True:  
    user_input = input("") 
    print(f"User 👨: {user_input}") 
    if user_input.lower() == 'exit':  
        break  
    first_prompt = False if conv_cnt>0 else True      
    conversation_history.append({'role':'user', 'content':user_input})

    
    answer = ''
    txt = ''
    
    max_attempts = 5
    errors = []
    attempts = 0  
    while attempts < max_attempts:  
        try:  
            
            txt = generate_text(sys_msg,user_input,conversation_history)
            code = extract_code(txt)
            if first_prompt:
                
                
                code = comment_out_lines(code,print_drop = True,data_drop = False)
            else:
                code = comment_out_lines(code,print_drop = True,data_drop = True)
            if attempts>0:
                code = comment_out_lines(code,print_drop = True,data_drop = True)
            
            exec(code)
            # append to history only if successfull
            conversation_history.append({'role':'assistant', 'content':txt})
            conv_cnt += 1
            break  
            # Return the result if operation is successful  
        except Exception as e:  
            errors.append(f"Attempt {attempts + 1} failed: {e}")  
            attempts += 1  
    
    sys_error = """
    You are an assistant that informs the user when their input is unclear, 
    and you ask them to provide more details or rephrase their message in the same language they used.
    """

    # generate anwer for failures
    if attempts == max_attempts:
        answer = generate_text(sys_error,user_input,conversation_history)
    



    # Stream the answer text  
    stream_text(answer, delay=0.01)   