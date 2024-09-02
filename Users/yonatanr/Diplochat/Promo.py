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
price = st.number_input("Price:", min_value=0.0, max_value=100.0, step=0.1)

# Calculate discount price
discount_price = (1 - discount_percentage / 100) * price

gondola_heads = st.number_input("Gondola Heads:", min_value=0, max_value=10)

promo_options = ['Coupon', 'Weekly', 'End of Season']
promo_type = st.radio("Promo Type:", promo_options)

leaf_options = ['True', 'False']
in_leaflet = st.selectbox("In Leaflet:", leaf_options)

# Initialize session state to store promotions if it doesn't exist
if "promotions" not in st.session_state:
    st.session_state.promotions = []

# Button to add the promotion
if st.button("Add Promotion"):
    # Create a dictionary with the entered promotion data
    new_promotion = {
        "Promotion Name": promotion_name,
        "Start Date": start_date,
        "End Date": end_date,
        "Discount Percentage": discount_percentage,
        "Description": description,
        "Price": price,
        "Discount Price": discount_price,
        "Gondola Heads": gondola_heads,
        "Promo Type": promo_type,
        "In Leaflet": in_leaflet,
    }

    # Append the new promotion to the session state list
    st.session_state.promotions.append(new_promotion)

    # Clear the input fields after adding
    st.success("Promotion added successfully!")

# Display the accumulated promotions in a dataframe
if st.session_state.promotions:
    df = pd.DataFrame(st.session_state.promotions)

    # Display the dataframe
    st.subheader("Generated Promotions Dataframe")
    st.dataframe(df)