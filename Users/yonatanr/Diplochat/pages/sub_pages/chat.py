## Yonatan GPT App

import streamlit as st
from openai import AzureOpenAI
# from openai import OpenAI
from audio_recorder_streamlit import audio_recorder
import requests
import re
import io
import os

whisper_api_key = os.getenv('WHISPER_OPENAI_KEY')
tts_api_key = os.getenv('TTS_OPENAI_KEY')
openai_api_key = os.getenv('OPENAI_KEY')


def run():

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


    user_name = st.session_state.get("name", "Guest")
    sys_message = f"""
    You Are a helpful AI Data Analyst Assistant for Diplomat Distributors LTD - Youre offical name is: Diplo-chat.

    The current user's name is {user_name}, When he greets you with "hi," be sure to include his first name in your response.

    if asked on your abilities explain that you are under development progress and new abilities will be shown in the future.

    About your model version:

    Your gpt versions name is GPT-4o mini released in july 18th 2024, and here is some info on it:

    GPT-4o ("o" for "omni") and GPT-4o mini are natively multimodal models designed to handle a combination of text, audio, and video inputs, and can generate outputs in text, audio, and image formats. GPT-4o mini is the lightweight version of GPT-4o.

    Background
    Before GPT-4o, users could interact with ChatGPT using Voice Mode, which operated with three separate models. GPT-4o integrates these capabilities into a single model that's trained across text, vision, and audio. This unified approach ensures that all inputs — whether text, visual, or auditory — are processed cohesively by the same neural network.

    GPT-4o mini is the next iteration of this omni model family, available in a smaller and cheaper version. This model offers higher accuracy than GPT-3.5 Turbo while being just as fast and supporting multimodal inputs and outputs.
    
    Please ensure your responses are concise, limited to 50 words or fewer.
    """

    st.title("Diplomat's AI Assistant 🤖")

    
    user_avatar = '🧑'



    # with st.sidebar:
    #     openai_api_key = st.text_input("OpenAI API Key", key="chatbot_api_key", type="password")

    # client = OpenAI(api_key=openai_api_key)
    client = AzureOpenAI(
    azure_endpoint = "https://ai-usa.openai.azure.com/", 
    api_key=openai_api_key,  
    api_version="2024-02-15-preview"
    )
    MODEL="Diplochat"
    if "openai_model" not in st.session_state:
        # st.session_state["openai_model"] = "gpt-4o-mini"
        st.session_state["openai_model"] = MODEL

    if "messages" not in st.session_state:
        st.session_state.messages = [{"role": "system", "content": sys_message}]

    # Display chat messages from history on app rerun
    for message in st.session_state.messages:
        
        if message["role"]=='assistant':
            with st.chat_message(message["role"], avatar='🤖'):
                st.markdown(message["content"])
        elif message["role"]=='user':
            with st.chat_message(message["role"], avatar=user_avatar):
                st.markdown(message["content"])




    # if prompt := st.chat_input("Ask me anything"):
    # Audio recorder  

    # Placeholder for audio recorder  
    # audio_placeholder = st.empty()  
    
    # Audio recording logic  
    # audio_bytes = audio_recorder(icon_size="3x", placeholder=audio_placeholder) 
    
    # if audio_bytes:=audio_recorder(icon_size="3x"):
    # Audio recording logic  
    with st.expander("Audio Recorder", expanded=True):  
        audio_bytes = audio_recorder(icon_size="3x",pause_threshold=2.0)  
    
        if audio_bytes:  
    
    # if audio_bytes:#=audio_recorder(icon_size="3x"):
    
            # audio_bytes = audio_recorder(icon_size="3x")
            transcribed_txt = transcribe_audio(audio_bytes)

            st.session_state.messages.append({"role": "user", "content": transcribed_txt})
            with st.chat_message("user", avatar=user_avatar):
                st.markdown(transcribed_txt)


            with st.chat_message("assistant", avatar='🤖'):
                with st.spinner("Thinking..."):
                    stream = client.chat.completions.create(
                        model=st.session_state["openai_model"],
                        messages=[
                            {"role": m["role"], "content": m["content"]}
                            for m in st.session_state.messages
                        ],
                        max_tokens=500,
                        stream=False,
                    )
                    # response = st.write_stream(stream)
                    
                    assistant_txt = stream.choices[0].message.content
                    response = st.write(assistant_txt)
                    
                st.session_state.messages.append({"role": "assistant", "content": assistant_txt})

                audio_content = text_to_speech(assistant_txt)
                st.audio(audio_content, format='audio/mp3', autoplay=True)

                
                
                