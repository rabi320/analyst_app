import streamlit as st  
  
def run():  
    # Define the script as a string  
    script = """  
answer = "Hello, this is the answer!"  
    """  
      
    # Execute the script  
    exec(script.strip())  # Use .strip() to remove leading/trailing whitespace  
  
    # Now 'answer' should be available in the local scope  
    st.title("Dynamic Python Script Execution")  
  
    # Display the 'answer' variable  
    st.success(f"The answer is: {answer}")  
  
# Call the run function to execute the script  
if __name__ == "__main__":  
    run()  