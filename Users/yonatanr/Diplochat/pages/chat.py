## Yonatan GPT App

import streamlit as st
from openai import AzureOpenAI
# from openai import OpenAI


def run():
    sys_message = """
    You Are a helpful AI Data Analyst Assistant for Diplomat Distributors LTD - Youre offical name is: Diplochat.

    if asked on your abilities explain that you are under development progress and new abilities will be shown in the future.

    About your model version:

    Your gpt versions name is GPT-4o mini released in july 18th 2024, and here is some info on it:

    GPT-4o ("o" for "omni") and GPT-4o mini are natively multimodal models designed to handle a combination of text, audio, and video inputs, and can generate outputs in text, audio, and image formats. GPT-4o mini is the lightweight version of GPT-4o.

    Background
    Before GPT-4o, users could interact with ChatGPT using Voice Mode, which operated with three separate models. GPT-4o integrates these capabilities into a single model that's trained across text, vision, and audio. This unified approach ensures that all inputs — whether text, visual, or auditory — are processed cohesively by the same neural network.

    GPT-4o mini is the next iteration of this omni model family, available in a smaller and cheaper version. This model offers higher accuracy than GPT-3.5 Turbo while being just as fast and supporting multimodal inputs and outputs.
    """

    st.title("Diplomat's AI Assistant 🤖")

    st.markdown("![](https://www.diplomat-global.com/wp-content/uploads/2018/06/logo.png)")
    user_avatar = '🧑'



    # with st.sidebar:
    #     openai_api_key = st.text_input("OpenAI API Key", key="chatbot_api_key", type="password")

    # client = OpenAI(api_key=openai_api_key)
    client = AzureOpenAI(
    azure_endpoint = "https://ai-usa.openai.azure.com/", 
    api_key='86bedc710e5e493290cb2b0ce6f16d80',  
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

    if prompt := st.chat_input("Ask me anything"):
        st.session_state.messages.append({"role": "user", "content": prompt})
        with st.chat_message("user", avatar=user_avatar):
            st.markdown(prompt)

        with st.chat_message("assistant", avatar='🤖'):
            with st.spinner("Thinking..."):
                stream = client.chat.completions.create(
                    model=st.session_state["openai_model"],
                    messages=[
                        {"role": m["role"], "content": m["content"]}
                        for m in st.session_state.messages
                    ],
                    max_tokens=500,
                    stream=True,
                )
            response = st.write_stream(stream)
        st.session_state.messages.append({"role": "assistant", "content": response})