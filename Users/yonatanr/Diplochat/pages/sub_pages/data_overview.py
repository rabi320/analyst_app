import streamlit as st  
import pandas as pd  
import pyodbc  
import requests
from audio_recorder_streamlit import audio_recorder
import re
import io
import os
db_password = os.getenv('DB_PASSWORD') 
whisper_api_key = os.getenv('WHISPER_OPENAI_KEY')
tts_api_key = os.getenv('TTS_OPENAI_KEY')

@st.cache_data
def load_data():
    # Connect to SQL Server
    conn = pyodbc.connect(driver='{ODBC Driver 17 for SQL Server}',
                          server='diplomat-analytics-server.database.windows.net',
                          database='NBO-DB',
                          uid='analyticsadmin', pwd=db_password)

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
    def transcribe_audio(audio_content: bytes) -> str:  
        """  
        Transcribe audio content using the Whisper API.  
        
        Parameters:  
        - audio_content (bytes): The audio file content as bytes.  
        - language (str): The language code for transcription (default is 'he' for Hebrew).  
        
        Returns:  
        - str: The transcription result.  
        """  
        
        # Define your endpoint and headers  
        endpoint = 'https://ai-yonatanrai434014214400.openai.azure.com/openai/deployments/whisper/audio/translations?api-version=2024-06-01'  
        headers = {  
            "api-key": whisper_api_key,  
        }  
    
        # Create a BytesIO object from the audio content  
        audio_stream = io.BytesIO(audio_content)  
    
        # Prepare the files parameter with the binary stream  
        files = {  
            'file': ('audio_file.wav', audio_stream, 'audio/wav')  # Use the correct MIME type for your audio  
        }  
    
    
        # Calling Azure OpenAI endpoint via REST API  
        response = requests.post(  
            url=endpoint,  
            headers=headers,  
            files=files  
        )  
    
        # Checking the response  
        if response.status_code == 200:  
            # Getting the transcription content 
            resp_txt = response.content.decode('utf-8') # Decode the response to a string  
            # Regex pattern to extract the text  
            pattern = r'"text": "(.*?)"'  
            
            # Using re.search to find the text  
            match = re.search(pattern, resp_txt)  
            
            if match:  
                # Extracting the text part  
                extracted_text = match.group(1)
            return extracted_text   
        else:  
            raise Exception(f"Error: {response.status_code} - {response.text}")      
    
    audio_bytes = audio_recorder(icon_size="3x")
    if audio_bytes:
        st.audio(audio_bytes, format="audio/wav")
    

        transcribed_txt = transcribe_audio(audio_bytes)
        st.write(transcribed_txt)

    st.subheader("Diplochat Future Voice🤖🔊")

    # Text to convert to speech  
    txt = 'מודל טקסט לדיבור שתומך בעברית'  
    
    def text_to_speech(txt):  
        # Preparing endpoint, headers, and request payload  
        endpoint = 'https://ai-yonatanrai933120347560.openai.azure.com/openai/deployments/tts-hd/audio/speech?api-version=2024-05-01-preview'  
        headers = {  
            "Content-Type": "application/json",  
            "api-key": tts_api_key,  
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