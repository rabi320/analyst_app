import streamlit as st
import pandas as pd

# Title for the app
st.title("Promotion Attribute Entry")

# Input fields for promotion attributes
promotion_name = st.text_input("Promotion Name:")
start_date = st.date_input("Start Date:")
end_date = st.date_input("End Date:")
discount_percentage = st.number_input("Discount Percentage:", min_value=0, max_value=100)
description = st.text_area("Description:")

# Button to add the promotion
if st.button("Add Promotion"):
    # Create a dictionary with the input data
    new_promotion = {
        "Promotion Name": promotion_name,
        "Start Date": start_date,
        "End Date": end_date,
        "Discount Percentage": discount_percentage,
        "Description": description,
    }

    # Create a DataFrame from the dictionary
    df = pd.DataFrame([new_promotion])

    # Display the DataFrame
    st.subheader("Generated Promotion DataFrame")
    st.dataframe(df)