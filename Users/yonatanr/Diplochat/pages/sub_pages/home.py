import streamlit as st

def run():
    st.header("Welcome to the Analytics Hub")
    # st.markdown('<p style="color: blue; font-size: 20px;"><strong>Committed to Your Success</strong></p>', unsafe_allow_html=True)
    # Adding stylized text with a cool gradient and shadow effect
    style_txt = """
    <h1 style="text-align: center; font-size: 20px; 
        color: cyan; 
        text-shadow: 2px 2px 4px rgba(0, 0, 0, 0.5); 
        background: linear-gradient(90deg, #0ff 70%, #e0f7fa);
        -webkit-background-clip: text; 
        -webkit-text-fill-color: transparent;">
        <strong>Committed to Your Success</strong>
        </h1>
    """
    st.markdown(style_txt, unsafe_allow_html=True)

    