import streamlit as st  
import pyodbc  
import pandas as pd 
from datetime import datetime

# Connect to SQL Server
conn = pyodbc.connect(driver='{ODBC Driver 17 for SQL Server}',
                        server='diplomat-analytics-server.database.windows.net',
                        database='NBO-DB',
                        uid='analyticsadmin', pwd='Analytics12345')


def run():  
    st.title("Dynamic Python Script Execution")  
  
    # Input area for the user to enter their code  
    script = st.text_area("Enter your Python code:", height=200,   
                           value='answer = "Hello, this is the answer!"')  # Default example code  
  
    if st.button("Run Code"):  
        # Create a local context for exec  
        local_context = {}  
  
        try:  
            # Execute the user's script  
            exec(script.strip(), {}, local_context)  # Pass local_context for variable storage  
              
            # Now 'answer' should be available in the local context  
            answer = local_context.get('answer', "No answer found.")  # Safely get answer  
              
            # Display the 'answer' variable  
            st.success(f"The answer is: {answer}")  
  
        except Exception as e:  
            # Display any error that occurs during execution  
            st.error(f"Error: {e}")  
  
# Call the run function to execute the script  
if __name__ == "__main__":  
    run()  
