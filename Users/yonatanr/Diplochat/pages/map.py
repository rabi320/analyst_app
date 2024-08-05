import streamlit as st
import folium

def display_map():
    st.title("Folium Map of Israel")

    # Create a Folium map centered around Israel
    m = folium.Map(location=[31.0461, 34.8516], zoom_start=7)

    # Add markers
    folium.Marker(location=[31.7683, 35.2137], popup='Jerusalem').add_to(m)
    folium.Marker(location=[32.0853, 34.7818], popup='Tel Aviv').add_to(m)
    folium.Marker(location=[29.5581, 34.9666], popup='Eilat').add_to(m)

    # Render the map in Streamlit
    st.write(m._repr_html_(), unsafe_allow_html=True)