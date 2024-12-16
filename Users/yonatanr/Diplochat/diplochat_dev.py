import streamlit as st

# Title of the app
st.title("Sign Up Page")

# Create a form for user input
with st.form(key='signup_form'):
    email = st.text_input("Email Address")
    full_name = st.text_input("Full Name")
    password = st.text_input("Password", type='password')

    submit_button = st.form_submit_button(label='Sign Up')

    if submit_button:
        # Display the entered information
        st.success("You have signed up successfully!")
        st.write("Email Address:", email)
        st.write("Full Name:", full_name)

        
        # It's not a good practice to display passwords, but for the sake of this example:
        st.write("Password:", password)