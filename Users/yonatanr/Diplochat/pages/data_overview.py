import streamlit as st  
import pandas as pd  
import pyodbc  
import requests
from audio_recorder_streamlit import audio_recorder

@st.cache_data
def load_data():
    # Connect to SQL Server
    conn = pyodbc.connect(driver='{ODBC Driver 17 for SQL Server}',
                          server='diplomat-analytics-server.database.windows.net',
                          database='NBO-DB',
                          uid='analyticsadmin', pwd='Analytics12345')

    query = 'SELECT * FROM [dbo].[DW_DIM_STORENEXT_BY_INDUSTRIES_ITEMS]'

    # Execute the query and read the results into a DataFrame  
    df = pd.read_sql_query(query, conn)  
    conn.close()

    return df

def run():
    st.header("Data Overview")

    # Load data
    df = load_data()

    # Display the title and the head of the DataFrame
    st.subheader("Table Head")
    st.write(df.head(10))

    # Optionally, you can add more features to display more information about the DataFrame
    st.subheader("Total Records")
    st.write(f"Total records in the dataset: {df.shape[0]}")

    st.subheader("DataFrame Columns")
    st.write(df.columns.tolist())

    st.subheader("Diplochat Future Ear 🤖🎙")
    
    
    audio_bytes = audio_recorder()
    if audio_bytes:
        st.audio(audio_bytes, format="audio/wav")

    st.subheader("Diplochat Future Voice🤖🔊")

    # Text to convert to speech  
    txt = 'מודל טקסט לדיבור שתומך בעברית'  
    
    def text_to_speech(txt):  
        # Preparing endpoint, headers, and request payload  
        endpoint = 'https://ai-yonatanrai933120347560.openai.azure.com/openai/deployments/tts-hd/audio/speech?api-version=2024-05-01-preview'  
        headers = {  
            "Content-Type": "application/json",  
            "api-key": "d2f7b5bf3799443e8217de45ea5ad734",  
        }  
        data = {  
            "model": "tts-hd",  
            "voice": "nova",  
            "input": txt  
        }  
    
        # Calling Azure OpenAI endpoint via REST API  
        response = requests.post(url=endpoint, headers=headers, json=data)  
    
        # Checking the response  
        if response.status_code == 200:  
            # Getting the audio content  
            audio_content = response.content  
            return audio_content  
        else:  
            st.error("Error fetching audio: " + str(response.status_code))  
            return None
    


    # Input text from the user  
    user_input = st.text_area("Enter text to convert to speech:")  
    
    if st.button("Convert to Speech"):  
        if user_input:  
            audio_content = text_to_speech(user_input)  
            if audio_content:  
                # Play audio directly from bytes  
                st.audio(audio_content, format='audio/mp3', autoplay=True)  