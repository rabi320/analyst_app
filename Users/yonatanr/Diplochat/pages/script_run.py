import streamlit as st  
  
def run():  
    # Define the script as a string  
    script = """  
answer = "Hello, this is the answer!"  
    """  
      
    # Create a local context for exec  
    local_context = {}  
      
    # Execute the script  
    exec(script.strip(), {}, local_context)  # Pass local_context for variable storage  
  
    # Now 'answer' should be available in the local context  
    answer = local_context.get('answer', "No answer found.")  # Safely get answer  
      
    st.title("Dynamic Python Script Execution")  
      
    # Display the 'answer' variable  
    st.success(f"The answer is: {answer}")  
  
# Call the run function to execute the script  
if __name__ == "__main__":  
    run()  
