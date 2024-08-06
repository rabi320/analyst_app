import streamlit as st  
from openai import AzureOpenAI  
import pandas as pd  
import re  
import time  
import warnings  
  
# Suppress all warnings  
warnings.filterwarnings('ignore')  
  
sys_msg = """  
You are an AI Data Analyst assistant For DIPLOMAT DISTRIBUTORS (1968) LTD, You are coding in python.  
...  
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
  
def run():  
    st.title("Diplomat's AI Analyst 🤖")  
  
    # Disclaimer  
    disclaimer = """  
    Disclaimer:  
    The information provided by this AI assistant is generated based on available data and patterns, and it may not always be accurate or up-to-date.  
    Users are advised to independently verify any critical information and exercise their judgment when relying on the assistant's responses.  
    The developers and creators of this AI assistant are not liable for any inaccuracies, errors, or consequences resulting from the use of the provided information.  
    """  
    st.markdown(disclaimer)  
  
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
  
            while attempts < max_attempts:  
                try:  
                    txt = generate_text(prompt, sys_msg, st.session_state.messages)  
                    code = extract_code(txt)  
                    if attempts == 0:  
                        code = comment_out_lines(code, print_drop=True, data_drop=False)  
                    else:  
                        code = comment_out_lines(code, print_drop=True, data_drop=True)  
                      
                    exec(code)  
                    # Append to history only if successful  
                    st.session_state.messages.append({'role': 'assistant', 'content': txt})  
  
                    # Simulate streaming by breaking response into smaller parts  
                    for i in range(0, len(txt), 10):  # Adjust the chunk size as needed  
                        chunk = txt[i:i+10]  
                        response_text += chunk  
                        response_placeholder.markdown(response_text)  
                        time.sleep(0.1)  # Adjust delay as needed  
  
                    break  
                except Exception as e:  
                    errors.append(f"Attempt {attempts + 1} failed: {e}")  
                    attempts += 1  
  
            sys_error = """  
            You are an assistant that informs the user when their input is unclear,  
            and you ask them to provide more details or rephrase their message in the same language they used.  
            """  
  
            if attempts == max_attempts:  
                answer = generate_text(sys_error, prompt, st.session_state.messages)  
                 # Simulate streaming for the final response  
                response_placeholder = st.empty()  
                response_text = ""  
                for i in range(0, len(answer), 10):  # Adjust the chunk size as needed  
                    chunk = answer[i:i+10]  
                    response_text += chunk  
                    response_placeholder.markdown(response_text)  
                    time.sleep(0.1)  # Adjust delay as needed 