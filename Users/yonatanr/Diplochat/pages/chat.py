## Yonatan GPT App

from openai import OpenAI
import streamlit as st


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

    # Disclaimer
    disclaimer = """
    **Disclaimer:**

    The information provided by this AI assistant is generated based on available data and patterns, and it may not always be accurate or up-to-date. 
    Users are advised to independently verify any critical information and exercise their judgment when relying on the assistant's responses. 
    The developers and creators of this AI assistant are not liable for any inaccuracies, errors, or consequences resulting from the use of the provided information.
    """

    # Display the disclaimer
    st.markdown(disclaimer)

    uploaded_file = st.file_uploader("Choose an Avatar for yourself: 📷🧑", type=["jpg", "jpeg", "png"])
    if uploaded_file is not None:
        image= uploaded_file.read()
        user_avatar = image
    else:
        user_avatar = '🧑'



    with st.sidebar:
        openai_api_key = st.text_input("OpenAI API Key", key="chatbot_api_key", type="password")

    client = OpenAI(api_key=openai_api_key)

    if "openai_model" not in st.session_state:
        st.session_state["openai_model"] = "gpt-4o-mini"

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