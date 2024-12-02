import streamlit as st
import msal
import os

# Configuration
CLIENT_ID = os.getenv('CLIENT_ID')
CLIENT_SECRET = os.getenv('CLIENT_SECRET')
TENANT = os.getenv('TENANT')
AUTHORITY = f'https://login.microsoftonline.com/{TENANT}'
REDIRECT_URI = 'http://localhost:8501/'
SCOPE = ['User.Read']

# Initialize the MSAL confidential client
app = msal.ConfidentialClientApplication(
    CLIENT_ID,
    authority=AUTHORITY,
    client_credential=CLIENT_SECRET,
)

def get_auth_url():
    return app.get_authorization_request_url(SCOPE, redirect_uri=REDIRECT_URI)

def get_token_from_code(code):
    result = app.acquire_token_by_authorization_code(code, scope=SCOPE, redirect_uri=REDIRECT_URI)
    return result

# Streamlit App
st.title("MSAL Authentication with Streamlit")

# Check for the authorization code in the URL
code = st.experimental_get_query_params().get('code', [None])[0]

if code:
    # User has authenticated
    token_response = get_token_from_code(code)
    if 'access_token' in token_response:
        st.success("Successfully authenticated!")
        st.write("Access Token:")
        st.code(token_response['access_token'])
    else:
        st.error("Authentication failed.")
        st.write(token_response.get("error"), token_response.get("error_description"))
else:
    # If no code, redirect the user to the login page
    auth_url = get_auth_url()
    st.markdown(f"[Login with Microsoft]({auth_url})")

# Additional content
st.write("This is a basic template for using MSAL with Streamlit.")