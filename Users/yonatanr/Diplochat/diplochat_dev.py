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
import base64
from io import BytesIO
import matplotlib.pyplot as plt
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

st.title('Diplochat')

# Clear the cache on app start or refresh
if 'cache_cleared' not in st.session_state:
    st.cache_data.clear()
    st.session_state.cache_cleared = True

# Login widget  
authentication_status = authenticator.login()  

# hide_pages([ "Map"])

# Adjusted authentication status handling  
if st.session_state['authentication_status']:  
    
    #st.sidebar.markdown("![](https://www.diplomat-global.com/wp-content/uploads/2018/06/logo.png)")
    authenticator.logout(location = 'sidebar')  # Add logout functionality  
    st.write(f'Welcome *{st.session_state["name"]}*')  # Display welcome message  



    if 'resolution_type' not in st.session_state:
        st.session_state.resolution_type = "monthly"  # default value

    
    
    # Sidebar radio button for choosing resolution type
    selected_resolution = st.sidebar.radio("Choose resolution:", ["monthly", "weekly"], index=0 if st.session_state.resolution_type == "monthly" else 1)

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
            - `SALES_ORGANIZATION_CODE`: The id of Diplomat's buisness unit, values: '1000' - Israel, '5000' - Georgia, '8000' - South Africa, 'NZ00' - New Zeeland.
            - `MATERIAL_CODE`: The id of Diplomat's items.
            - `INDUSTRY_CODE`:  The id of Diplomat's different industries that relate to their customers.
            - 'CUSTOMER_CODE': The id of the exact customers.
            - 'Gross': The gross sales.
            - 'Net': The net sales.
            - 'Net VAT': The net sales with tax.
            - 'Gross VAT': The gross sales with tax.
            - 'Units': the number of units.
            - **Note**: This data relates to the sell in of diplomat and needs the material barcode from the material table to connect to external data like chp and others. 

        
    6. **DW_DIM_CUSTOMERS** ('customer_df'):
        - **Description**: The customers's information.
            - 'CUSTOMER_CODE': The id of the exact customers (primary key).
            - 'CUSTOMER':  Customer name.
            - 'CITY':  City of the customer.
            - 'CUSTOMER_ADDRESS':  Adress of the customer.
            - 'CUST_LATITUDE': Latitude coordinate of the customer.
            - 'CUST_LONGITUDE': Longitude coordinate of the customer.
             **Note**: This data relates to the invoices table, can merge to add the data of the invoices over the customer code.
            
    7. **DW_DIM_INDUSTRIES** ('industry_df'):
        - **Description**: The industries and their names.
            - `INDUSTRY`:  Industry name.
            - `INDUSTRY_CODE`:  The id of Diplomat's different industries  (primary key).
        **Note**: This data relates to the invoices table, can merge to add the data of the invoices over the industry code.
            

    8. **DW_DIM_MATERIAL** ('material_df')
        - **Description**: The materials and their attributes.
            - `MATERIAL_NUMBER`: The id of Diplomat's items (primary key). 
            - `MATERIAL_EN`: Item name in english. 
            - `MATERIAL_HE`: Item name in hebrew.
            - `MATERIAL_DIVISION`: Type of item (mainly food ot toiletics).
            - 'BRAND_HEB': The brand of the item in hebrew.
            - 'BRAND_ENG': The brand of the item in english.
            - 'SUB_BRAND_HEB': The sub brand of the item in hebrew.
            - 'SUB_BRAND_ENG': The sub brand of the item in english.            
            - 'CATEGORY_HEB': The category of the item in hebrew.
            - 'CATEGORY_ENG': The category of the item in english.
            - 'SUPPLIER_HEB': The supplier of the item in hebrew.
            - 'SUPPLIER_ENG': The supplier of the item in english.
            - 'BARCODE_EA': the barcode of a single item.
            - 'SALES_UNIT': the item's sales unit.
            - 'BOXING_SIZE': the item's number of single units being sold in the sales unit.
        **Note**: This data relates to the invoices table, can merge to add the data of the invoices over the material code/ number.
            
        
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
            ,
            'DW_DIM_CUSTOMERS':\"\"\"
            [Query]
            \"\"\",
            'DW_DIM_INDUSTRIES':
            \"\"\"
            [Query]
            \"\"\",
            'DW_DIM_MATERIAL':
            \"\"\"
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
    customer_df = dataframes['DW_DIM_CUSTOMERS']
    industry_df = dataframes['DW_DIM_INDUSTRIES']
    material_df = dataframes['DW_DIM_MATERIAL']

    # Convert date columns to datetime
    stnx_sales['Day'] = pd.to_datetime(stnx_sales['Day'])
    chp['DATE'] = pd.to_datetime(chp['DATE'])
    dt_df['DATE'] = pd.to_datetime(dt_df['DATE'])
    inv_df['DATE] = pd.to_datetime(inv_df['DATE'])
    ```

    The names of the brands of diplomat-
        אולוויז, אולדייז, פנטן, סאקלה, פמפרס, טמפקס, אריאל, Fairy, אוראל בי, הד&שולדרס, קוטדור, לוטוס, טייד, לנור, קראפט, מילקה, LU, סקיני טינס, קולמנס, גולדן בריק, HP, אוראו, וולה, אוריינטל פוד, דורסל, Skippy, פרינגלס, קיקומן, גילט, קולסטון נטורלס, הרבל אסנס, Walkers, ג'השאן , בראון, קולסטון קיט, אוזי, בונזו, שזיר, סטארקיסט, הלטי בוי, רומו, Lazaro, ביונד מיט, לור, נוטרילון, גיקובס, מזולה, סופר קאט, היינץ, לחם יין, קלוגס, לה קט, Lo Salt, SUPREME, ויוה קולור, נטורלה, רג'יה, קרסט, אולד ספייס, קולסטון-2000, ויולייף, דרפורד, טובלרון, מקסוול, אל ארז, דקורה, ביאסק, ריין דיז'ון,  - לא ידוע-1, וולהפלקס, יונה, פרופה, אורגניק אינדיה, נון, הרמזית, All In, קוסקא, Mission, יורוקיטי, דורות, נסיך הים, סיקרט, לה ניוקריה, סופר קט, יוניבר, פראוד, פטי, לגונה, קרם קולור, מנישביץ, מאיר את בייגל, קדבורי, גקובזי, דורו, מסטמכר, בארני, פנדה, קולסטון רוט, Arifoglu, בלובנד, מילוטל, פלנטרס, לוריאל, סופט קולור, OXO, מרום את קונפינו, 7 Days, קולסטון אינטנס, ציריו, וולה דלוקס, ויטקראפט, פורטוס, א. קנטינה, אופיסר, לאב דוג, משק ווילר, סוויטאנגו, איליי, אונלי, קאפוטו, אינאבה, סינגה
    
    Quesstions Convention - 

    For any question you provide a code in python and in the end give the the answer in a python text variable named 'answer' after making the needed analysis.

    * A very important note on Predictive analytics! - when asked about a future event, make a forecast based on forcasting methods such as SARIMA to get the desired prediction (make sure to deactivate any printed output from the chosen model).

    Context for the Questions of stakeholders:

    >Market Cap/Share (נתח שוק) - The Percent of total sales in NIS of a certain brand in his category by default or any other field if specifically requested by the user - 
    meaning if asked about a certain brand's market cap, then you need sum that brand's sales in the chosen time frame (Can be monthly, weekly or daily, group your dataframe accordingly), and devide it by the total sales in that brand's category,
    you need to merge the stnx_sales and stnx_items dataframes to obtain all the neccesary data for that.

    >textual data - all the textual data here is hebrew so take that in mind while filtering dataframes.

    >Competitors (מתחרים) - When requesting data about competitors, we are the supplier name 'דיפלומט' in the data and other supliers in the same category/ requested user's field are the competition. 

    >Promotion Sales (מבצעים) - It is an actual promotion only where the 'AVG_SELLOUT_PRICE' a non-negative float number value. 
    Final reminder: ensure that the 'answer' variable resembles a genuine prompt produced by a language model in the language used to address you!
    """
    sys_msg += f'\nYour operation present date is {datetime.now()}.'

    sys_error = """  
    You are an assistant that informs the user when their input is unclear,  
    and you ask them to provide more details or rephrase their message in the same language they used.  
    """  

    examples = [{'role':'user','content':"What is the market share of Always in monthly segmentation? Add gross and net sales to your analysis, next to the brand's market share"},
        {'role': 'assistant',
        'content': '```python\nbrand_name = \'אולוויז\'  \ncategories = stnx_items[stnx_items[\'Brand_Name\'] == brand_name][\'Category_Name\'].unique()  \n  \nif len(categories) > 1:  \n    # Iterate through each category  \n    category_market_shares = []  \n    for category in categories:  \n        print(category)  \n        # Merging sales data with items data to include brand attributes  \n        merged_data = stnx_sales.merge(stnx_items[stnx_items[\'Category_Name\'] == category], on=\'Barcode\', how=\'inner\')  \n          \n        # Filtering for the brand  \n        brand_data = merged_data[(merged_data[\'Brand_Name\'] == brand_name) & (merged_data[\'Supplier_Name\'] == \'דיפלומט\')]  \n\n        # Grouping by week and summing sales in NIS for the brand  \n        brand_monthly_sales = brand_data.resample(\'M\', on=\'Day\').agg({\'Sales_NIS\': \'sum\'}).reset_index()  \n\n        # Filtering sales data for the same category  \n        total_monthly_sales = merged_data.resample(\'M\', on=\'Day\').agg({\'Sales_NIS\': \'sum\'}).reset_index()  \n\n        # Merging the data to compute the market cap percentage  \n        market_cap_data = brand_monthly_sales.merge(total_monthly_sales, on=\'Day\', suffixes=(\'_Brand\', \'_Total\'))  \n\n        # Calculating market cap percentage  \n        market_cap_data[\'Market_Cap_Percent\'] = (market_cap_data[\'Sales_NIS_Brand\'] / market_cap_data[\'Sales_NIS_Total\']) * 100  \n\n        # Preparing the final result  \n        monthly_market_cap = market_cap_data[[\'Day\', \'Market_Cap_Percent\']]  \n\n        # Add invoice data  \n        brand_barcodes = brand_data.Barcode.unique().tolist()  \n        brand_material_codes = material_df[material_df.BARCODE_EA.isin(brand_barcodes)].MATERIAL_NUMBER.unique().tolist()  \n        brand_inv_df = inv_df[inv_df.MATERIAL_CODE.isin(brand_material_codes)]  \n\n        # Filter only to sales organization 1000 (Israel)  \n        brand_inv_df = brand_inv_df[brand_inv_df.SALES_ORGANIZATION_CODE == \'1000\']  \n        monthly_brand_inv_df = brand_inv_df.resample(\'M\', on=\'DATE\').agg({\'Gross\': \'sum\', \'Net\': \'sum\'}).reset_index()  \n\n        # Merging market cap data with invoice data  \n        merged_market_inv_data = monthly_market_cap.merge(monthly_brand_inv_df, left_on=\'Day\', right_on=\'DATE\', how=\'inner\', suffixes=(\'_Market\', \'_Invoice\')).drop(\'DATE\',axis =1)  \n\n        if len(merged_market_inv_data)==0:\n            continue\n\n        else:\n            # Final output for the category\n            one_merged_market_inv_data = merged_market_inv_data\n            brand_category = category  \n            category_result = f"Category: {category}\n{merged_market_inv_data.to_string(index=False)}"  \n            category_market_shares.append(category_result)  \n\n            \n\n    if len(category_market_shares)==0:    \n        answer = f"No records of sales diplomat related to {brand_name} brand in our records"  \n\n    else:\n        if len(category_market_shares)>1:\n            answer = f"The monthly data for {brand_name} in each category:\n\n" + "\n\n".join(category_market_shares)\n        else:\n            category_for_market_cap = brand_category\n            answer = f"The monthly data of {brand_name} in {category_for_market_cap} category:\n{one_merged_market_inv_data.to_string(index=False)}"    \nelse:  \n    # Merging sales data with items data to include brand attributes  \n    merged_data = stnx_sales.merge(stnx_items, on=\'Barcode\', how=\'inner\')  \n  \n    # Filtering for the brand and the supplier to diplomat  \n    brand_data = merged_data[(merged_data[\'Brand_Name\'] == brand_name) & (merged_data[\'Supplier_Name\'] == \'דיפלומט\')]  \n      \n    # Grouping by week and summing sales in NIS for the brand  \n    brand_monthly_sales = brand_data.resample(\'M\', on=\'Day\').agg({\'Sales_NIS\': \'sum\'}).reset_index()  \n      \n    # Get the category for the brand  \n    category_for_market_cap = brand_data[\'Category_Name\'].values[0]  \n  \n    # Filtering sales data for the same category  \n    total_monthly_sales = merged_data[merged_data[\'Category_Name\'] == category_for_market_cap].resample(\'M\', on=\'Day\').agg({\'Sales_NIS\': \'sum\'}).reset_index()  \n  \n    # Merging the data to compute the market cap percentage  \n    market_cap_data = brand_monthly_sales.merge(total_monthly_sales, on=\'Day\', suffixes=(\'_Brand\', \'_Total\'))  \n  \n    # Calculating market cap percentage  \n    market_cap_data[\'Market_Cap_Percent\'] = (market_cap_data[\'Sales_NIS_Brand\'] / market_cap_data[\'Sales_NIS_Total\']) * 100  \n  \n    # Preparing the final result  \n    monthly_market_cap = market_cap_data[[\'Day\', \'Market_Cap_Percent\']]  \n  \n    # Add invoice data  \n    brand_barcodes = brand_data.Barcode.unique().tolist()  \n    brand_material_codes = material_df[material_df.BARCODE_EA.isin(brand_barcodes)].MATERIAL_NUMBER.unique().tolist()  \n    brand_inv_df = inv_df[inv_df.MATERIAL_CODE.isin(brand_material_codes)]  \n  \n    # Filter only to sales organization 1000 (Israel)  \n    brand_inv_df = brand_inv_df[brand_inv_df.SALES_ORGANIZATION_CODE == \'1000\']  \n    monthly_brand_inv_df = brand_inv_df.resample(\'M\', on=\'DATE\').agg({\'Gross\': \'sum\', \'Net\': \'sum\'}).reset_index()  \n  \n    # Merging market cap data with invoice data  \n    merged_market_inv_data = monthly_market_cap.merge(monthly_brand_inv_df, left_on=\'Day\', right_on=\'DATE\', how=\'inner\', suffixes=(\'_Market\', \'_Invoice\')).drop(\'DATE\',axis =1)  \n\n    if len(merged_market_inv_data)==0:    \n        answer = f"No records of diplomat\'s sales related to {brand_name} brand in our records"  \n\n    else:\n        answer = f"The monthly data of {brand_name} in {category_for_market_cap} category:\n{merged_market_inv_data.to_string(index=False)}"   \n```\n'},
        {'role':'user','content':"What is Pringles market share in monthly segmentation? Please include both gross and net sales in your analysis alongside the brand's market share."},
        {'role': 'assistant',
        'content': '```python\nbrand_name = \'פרינגלס\'  \ncategories = stnx_items[stnx_items[\'Brand_Name\'] == brand_name][\'Category_Name\'].unique()  \n  \nif len(categories) > 1:  \n    # Iterate through each category  \n    category_market_shares = []  \n    for category in categories:  \n        print(category)  \n        # Merging sales data with items data to include brand attributes  \n        merged_data = stnx_sales.merge(stnx_items[stnx_items[\'Category_Name\'] == category], on=\'Barcode\', how=\'inner\')  \n          \n        # Filtering for the brand  \n        brand_data = merged_data[(merged_data[\'Brand_Name\'] == brand_name) & (merged_data[\'Supplier_Name\'] == \'דיפלומט\')]  \n\n        # Grouping by week and summing sales in NIS for the brand  \n        brand_monthly_sales = brand_data.resample(\'M\', on=\'Day\').agg({\'Sales_NIS\': \'sum\'}).reset_index()  \n\n        # Filtering sales data for the same category  \n        total_monthly_sales = merged_data.resample(\'M\', on=\'Day\').agg({\'Sales_NIS\': \'sum\'}).reset_index()  \n\n        # Merging the data to compute the market cap percentage  \n        market_cap_data = brand_monthly_sales.merge(total_monthly_sales, on=\'Day\', suffixes=(\'_Brand\', \'_Total\'))  \n\n        # Calculating market cap percentage  \n        market_cap_data[\'Market_Cap_Percent\'] = (market_cap_data[\'Sales_NIS_Brand\'] / market_cap_data[\'Sales_NIS_Total\']) * 100  \n\n        # Preparing the final result  \n        monthly_market_cap = market_cap_data[[\'Day\', \'Market_Cap_Percent\']]  \n\n        # Add invoice data  \n        brand_barcodes = brand_data.Barcode.unique().tolist()  \n        brand_material_codes = material_df[material_df.BARCODE_EA.isin(brand_barcodes)].MATERIAL_NUMBER.unique().tolist()  \n        brand_inv_df = inv_df[inv_df.MATERIAL_CODE.isin(brand_material_codes)]  \n\n        # Filter only to sales organization 1000 (Israel)  \n        brand_inv_df = brand_inv_df[brand_inv_df.SALES_ORGANIZATION_CODE == \'1000\']  \n        monthly_brand_inv_df = brand_inv_df.resample(\'M\', on=\'DATE\').agg({\'Gross\': \'sum\', \'Net\': \'sum\'}).reset_index()  \n\n        # Merging market cap data with invoice data  \n        merged_market_inv_data = monthly_market_cap.merge(monthly_brand_inv_df, left_on=\'Day\', right_on=\'DATE\', how=\'inner\', suffixes=(\'_Market\', \'_Invoice\')).drop(\'DATE\',axis =1)  \n\n        if len(merged_market_inv_data)==0:\n            continue\n\n        else:\n            # Final output for the category\n            one_merged_market_inv_data = merged_market_inv_data\n            brand_category = category  \n            category_result = f"Category: {category}\n{merged_market_inv_data.to_string(index=False)}"  \n            category_market_shares.append(category_result)  \n\n            \n\n    if len(category_market_shares)==0:    \n        answer = f"No records of sales diplomat related to {brand_name} brand in our records"  \n\n    else:\n        if len(category_market_shares)>1:\n            answer = f"The monthly data for {brand_name} in each category:\n\n" + "\n\n".join(category_market_shares)\n        else:\n            category_for_market_cap = brand_category\n            answer = f"The monthly data of {brand_name} in {category_for_market_cap} category:\n{one_merged_market_inv_data.to_string(index=False)}"    \nelse:  \n    # Merging sales data with items data to include brand attributes  \n    merged_data = stnx_sales.merge(stnx_items, on=\'Barcode\', how=\'inner\')  \n  \n    # Filtering for the brand and the supplier to diplomat  \n    brand_data = merged_data[(merged_data[\'Brand_Name\'] == brand_name) & (merged_data[\'Supplier_Name\'] == \'דיפלומט\')]  \n      \n    # Grouping by week and summing sales in NIS for the brand  \n    brand_monthly_sales = brand_data.resample(\'M\', on=\'Day\').agg({\'Sales_NIS\': \'sum\'}).reset_index()  \n      \n    # Get the category for the brand  \n    category_for_market_cap = brand_data[\'Category_Name\'].values[0]  \n  \n    # Filtering sales data for the same category  \n    total_monthly_sales = merged_data[merged_data[\'Category_Name\'] == category_for_market_cap].resample(\'M\', on=\'Day\').agg({\'Sales_NIS\': \'sum\'}).reset_index()  \n  \n    # Merging the data to compute the market cap percentage  \n    market_cap_data = brand_monthly_sales.merge(total_monthly_sales, on=\'Day\', suffixes=(\'_Brand\', \'_Total\'))  \n  \n    # Calculating market cap percentage  \n    market_cap_data[\'Market_Cap_Percent\'] = (market_cap_data[\'Sales_NIS_Brand\'] / market_cap_data[\'Sales_NIS_Total\']) * 100  \n  \n    # Preparing the final result  \n    monthly_market_cap = market_cap_data[[\'Day\', \'Market_Cap_Percent\']]  \n  \n    # Add invoice data  \n    brand_barcodes = brand_data.Barcode.unique().tolist()  \n    brand_material_codes = material_df[material_df.BARCODE_EA.isin(brand_barcodes)].MATERIAL_NUMBER.unique().tolist()  \n    brand_inv_df = inv_df[inv_df.MATERIAL_CODE.isin(brand_material_codes)]  \n  \n    # Filter only to sales organization 1000 (Israel)  \n    brand_inv_df = brand_inv_df[brand_inv_df.SALES_ORGANIZATION_CODE == \'1000\']  \n    monthly_brand_inv_df = brand_inv_df.resample(\'M\', on=\'DATE\').agg({\'Gross\': \'sum\', \'Net\': \'sum\'}).reset_index()  \n  \n    # Merging market cap data with invoice data  \n    merged_market_inv_data = monthly_market_cap.merge(monthly_brand_inv_df, left_on=\'Day\', right_on=\'DATE\', how=\'inner\', suffixes=(\'_Market\', \'_Invoice\')).drop(\'DATE\',axis =1)  \n\n    if len(merged_market_inv_data)==0:    \n        answer = f"No records of diplomat\'s sales related to {brand_name} brand in our records"  \n\n    else:\n        answer = f"The monthly data of {brand_name} in {category_for_market_cap} category:\n{merged_market_inv_data.to_string(index=False)}"   \n```\n'},
        {'role':'user','content':"What is Oreo's market share in weekly segmentation? Additionally, please provide an analysis that includes both gross and net sales alongside the brand's market share."},
        {'role': 'assistant',
        'content': '```python\nbrand_name = \'אוראו\'  \ncategories = stnx_items[stnx_items[\'Brand_Name\'] == brand_name][\'Category_Name\'].unique()  \n  \nif len(categories) > 1:  \n    # Iterate through each category  \n    category_market_shares = []  \n    for category in categories:  \n        print(category)  \n        # Merging sales data with items data to include brand attributes  \n        merged_data = stnx_sales.merge(stnx_items[stnx_items[\'Category_Name\'] == category], on=\'Barcode\', how=\'inner\')  \n          \n        # Filtering for the brand  \n        brand_data = merged_data[(merged_data[\'Brand_Name\'] == brand_name) & (merged_data[\'Supplier_Name\'] == \'דיפלומט\')]  \n\n        # Grouping by week and summing sales in NIS for the brand  \n        brand_monthly_sales = brand_data.resample(\'M\', on=\'Day\').agg({\'Sales_NIS\': \'sum\'}).reset_index()  \n\n        # Filtering sales data for the same category  \n        total_monthly_sales = merged_data.resample(\'M\', on=\'Day\').agg({\'Sales_NIS\': \'sum\'}).reset_index()  \n\n        # Merging the data to compute the market cap percentage  \n        market_cap_data = brand_monthly_sales.merge(total_monthly_sales, on=\'Day\', suffixes=(\'_Brand\', \'_Total\'))  \n\n        # Calculating market cap percentage  \n        market_cap_data[\'Market_Cap_Percent\'] = (market_cap_data[\'Sales_NIS_Brand\'] / market_cap_data[\'Sales_NIS_Total\']) * 100  \n\n        # Preparing the final result  \n        monthly_market_cap = market_cap_data[[\'Day\', \'Market_Cap_Percent\']]  \n\n        # Add invoice data  \n        brand_barcodes = brand_data.Barcode.unique().tolist()  \n        brand_material_codes = material_df[material_df.BARCODE_EA.isin(brand_barcodes)].MATERIAL_NUMBER.unique().tolist()  \n        brand_inv_df = inv_df[inv_df.MATERIAL_CODE.isin(brand_material_codes)]  \n\n        # Filter only to sales organization 1000 (Israel)  \n        brand_inv_df = brand_inv_df[brand_inv_df.SALES_ORGANIZATION_CODE == \'1000\']  \n        monthly_brand_inv_df = brand_inv_df.resample(\'M\', on=\'DATE\').agg({\'Gross\': \'sum\', \'Net\': \'sum\'}).reset_index()  \n\n        # Merging market cap data with invoice data  \n        merged_market_inv_data = monthly_market_cap.merge(monthly_brand_inv_df, left_on=\'Day\', right_on=\'DATE\', how=\'inner\', suffixes=(\'_Market\', \'_Invoice\')).drop(\'DATE\',axis =1)  \n\n        if len(merged_market_inv_data)==0:\n            continue\n\n        else:\n            # Final output for the category\n            one_merged_market_inv_data = merged_market_inv_data\n            brand_category = category  \n            category_result = f"Category: {category}\n{merged_market_inv_data.to_string(index=False)}"  \n            category_market_shares.append(category_result)  \n\n            \n\n    if len(category_market_shares)==0:    \n        answer = f"No records of sales diplomat related to {brand_name} brand in our records"  \n\n    else:\n        if len(category_market_shares)>1:\n            answer = f"The monthly data for {brand_name} in each category:\n\n" + "\n\n".join(category_market_shares)\n        else:\n            category_for_market_cap = brand_category\n            answer = f"The monthly data of {brand_name} in {category_for_market_cap} category:\n{one_merged_market_inv_data.to_string(index=False)}"    \nelse:  \n    # Merging sales data with items data to include brand attributes  \n    merged_data = stnx_sales.merge(stnx_items, on=\'Barcode\', how=\'inner\')  \n  \n    # Filtering for the brand and the supplier to diplomat  \n    brand_data = merged_data[(merged_data[\'Brand_Name\'] == brand_name) & (merged_data[\'Supplier_Name\'] == \'דיפלומט\')]  \n      \n    # Grouping by week and summing sales in NIS for the brand  \n    brand_monthly_sales = brand_data.resample(\'M\', on=\'Day\').agg({\'Sales_NIS\': \'sum\'}).reset_index()  \n      \n    # Get the category for the brand  \n    category_for_market_cap = brand_data[\'Category_Name\'].values[0]  \n  \n    # Filtering sales data for the same category  \n    total_monthly_sales = merged_data[merged_data[\'Category_Name\'] == category_for_market_cap].resample(\'M\', on=\'Day\').agg({\'Sales_NIS\': \'sum\'}).reset_index()  \n  \n    # Merging the data to compute the market cap percentage  \n    market_cap_data = brand_monthly_sales.merge(total_monthly_sales, on=\'Day\', suffixes=(\'_Brand\', \'_Total\'))  \n  \n    # Calculating market cap percentage  \n    market_cap_data[\'Market_Cap_Percent\'] = (market_cap_data[\'Sales_NIS_Brand\'] / market_cap_data[\'Sales_NIS_Total\']) * 100  \n  \n    # Preparing the final result  \n    monthly_market_cap = market_cap_data[[\'Day\', \'Market_Cap_Percent\']]  \n  \n    # Add invoice data  \n    brand_barcodes = brand_data.Barcode.unique().tolist()  \n    brand_material_codes = material_df[material_df.BARCODE_EA.isin(brand_barcodes)].MATERIAL_NUMBER.unique().tolist()  \n    brand_inv_df = inv_df[inv_df.MATERIAL_CODE.isin(brand_material_codes)]  \n  \n    # Filter only to sales organization 1000 (Israel)  \n    brand_inv_df = brand_inv_df[brand_inv_df.SALES_ORGANIZATION_CODE == \'1000\']  \n    monthly_brand_inv_df = brand_inv_df.resample(\'M\', on=\'DATE\').agg({\'Gross\': \'sum\', \'Net\': \'sum\'}).reset_index()  \n  \n    # Merging market cap data with invoice data  \n    merged_market_inv_data = monthly_market_cap.merge(monthly_brand_inv_df, left_on=\'Day\', right_on=\'DATE\', how=\'inner\', suffixes=(\'_Market\', \'_Invoice\')).drop(\'DATE\',axis =1)  \n\n    if len(merged_market_inv_data)==0:    \n        answer = f"No records of diplomat\'s sales related to {brand_name} brand in our records"  \n\n    else:\n        answer = f"The monthly data of {brand_name} in {category_for_market_cap} category:\n{merged_market_inv_data.to_string(index=False)}"   \n```\n'},
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
                --WHERE [DATE] BETWEEN DATEADD(DAY, -90, GETDATE()) AND GETDATE()
            """,
            'DW_DIM_CUSTOMERS':
            """
            SELECT [CUSTOMER_CODE],
            [CUSTOMER],
            [CITY],
            [CUSTOMER_ADDRESS],
            [CUST_LATITUDE],
            [CUST_LONGITUDE]
            FROM [dbo].[DW_DIM_CUSTOMERS]
            WHERE CUSTOMER_CODE IN (SELECT DISTINCT CUSTOMER_CODE FROM [dbo].[AGGR_MONTHLY_DW_INVOICES])
            """
            ,
            'DW_DIM_INDUSTRIES':
            """
            SELECT [INDUSTRY]
                ,[INDUSTRY_CODE]
            FROM [dbo].[DW_DIM_INDUSTRIES]
            """
            ,
            'DW_DIM_MATERIAL':
            """
            SELECT [MATERIAL_NUMBER]
                ,[MATERIAL_EN]
                ,[MATERIAL_HE]
                ,[MATERIAL_DIVISION]
                ,[BRAND_HEB]
                ,[BRAND_ENG]
                ,[SUB_BRAND_HEB]
                ,[SUB_BRAND_ENG]
                ,[CATEGORY_HEB]
                ,[CATEGORY_ENG]
                ,[BARCODE_EA]
	            ,[SALES_UNIT]
	            ,[BOXING_SIZE]
            FROM [dbo].[DW_DIM_MATERIAL] 
            WHERE MATERIAL_NUMBER IN (SELECT DISTINCT MATERIAL_CODE FROM [dbo].[AGGR_MONTHLY_DW_INVOICES])
            """
            ,
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
        # text_or_regex  = '👍'
        text_or_regex  = r'(?=.*👍)(?=.*מצוין עבור הנהלת הקבוצה)'  
        
        liked_example_cond = log_df.User_Ratings.str.contains(text_or_regex, regex=True)  
    
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
    customer_df = dataframes['DW_DIM_CUSTOMERS']
    industry_df = dataframes['DW_DIM_INDUSTRIES']
    material_df = dataframes['DW_DIM_MATERIAL']
    dt_df = dataframes['DATE_HOLIAY_DATA']
    log_df = dataframes['AI_LOG']

    # Convert date columns to datetime
    stnx_sales['Day'] = pd.to_datetime(stnx_sales['Day'])
    dt_df['DATE'] = pd.to_datetime(dt_df['DATE'])

    # duplication drop
    customer_df = customer_df.drop_duplicates(subset = ['CUSTOMER_CODE'])
    material_df = material_df.drop_duplicates(subset = ['MATERIAL_NUMBER'])

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
                st.markdown(message["content"], unsafe_allow_html=True)

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

        # base history
        # st.session_state.base_history[1:1+st.session_state.n_most_similar*2] = dynamic_examples_lst
        
        # training base history - premade examples
        st.session_state.base_history[1:1+st.session_state.n_most_similar*2] = examples

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
                        local_context = {'chp':chp,'stnx_sales':stnx_sales,'stnx_items':stnx_items,'pd':pd,'np':np,'dt_df':dt_df,'SARIMAX':SARIMAX,'customer_df':customer_df,'industry_df':industry_df,'material_df':material_df,'base64':base64,'BytesIO':BytesIO,'plt':plt}
                    else:
                        local_context = {'stnx_sales':stnx_sales,'stnx_items':stnx_items,'pd':pd,'np':np,'dt_df':dt_df,'SARIMAX':SARIMAX,'inv_df':inv_df,'customer_df':customer_df,'industry_df':industry_df,'material_df':material_df,'base64':base64,'BytesIO':BytesIO,'plt':plt}

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

                    finally: ensure that the your response is in the language used by your recieved input, and is presenting information and insights to the user.
                    """
                    
                    answer_has_plot = 'data:image/png;base64' in answer

                    if not answer_has_plot:
                        decorator_response = model_reponse(answer, sys_decorator)
                        answer = decorator_response.choices[0].message.content.strip()
                        
                        decorator_usage_dict = decorator_response.to_dict()['usage']
                        error_usage_dict = {'completion_tokens': 0, 'prompt_tokens': 0, 'total_tokens': 0}
                        
                        n_llm_api_call+=1

                    else:
                        decorator_usage_dict = {'completion_tokens': 0, 'prompt_tokens': 0, 'total_tokens': 0}
                        error_usage_dict = {'completion_tokens': 0, 'prompt_tokens': 0, 'total_tokens': 0}

                    history_msg = f"```python{code}```"

                    with st.chat_message("assistant", avatar='🤖'):
                    
                        if not answer_has_plot:
                            # Create a placeholder for streaming output 
                            placeholder = st.empty()  
                            streamed_text = ""  

                            # Stream the answer output  
                            for char in answer:  
                                streamed_text += char  
                                placeholder.markdown(streamed_text)  
                                time.sleep(0.01)  # Adjust the sleep time to control the streaming speed 
                        else:
                            # Create a placeholder for streaming output 
                            placeholder = st.empty()  
                            streamed_text = ""  

                            # Stream the answer output  
                            for char in answer:
                                streamed_text += char
                                placeholder.markdown(streamed_text)

                                # Check if streamed_text endswith ('<img src=')
                                if streamed_text.endswith('<img src='):
                                    # Display the remainder of the answer starting from the current position
                                    streamed_text+=answer[len(streamed_text):]
                                    placeholder.markdown(streamed_text,unsafe_allow_html=True)
                                    break
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

    
    


