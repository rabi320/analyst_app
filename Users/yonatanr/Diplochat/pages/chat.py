## Yonatan GPT App

from openai import OpenAI
import streamlit as st



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