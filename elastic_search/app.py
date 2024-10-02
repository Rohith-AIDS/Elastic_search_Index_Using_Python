import pandas as pd
from elasticsearch import Elasticsearch
import streamlit as st

# Function to load employee data
def load_employee_data(file_path):
    try:
        data = pd.read_csv(file_path)
        return data
    except UnicodeDecodeError:
        data = pd.read_csv(file_path, encoding='ISO-8859-1')  # Handle encoding issues
        return data

# Connect to Elasticsearch
es = Elasticsearch(
    ['https://localhost:9200'],
    http_auth=('elastic', 'vDIyQ6NMHJ9Wpm_gUlzB'),
    verify_certs=False
)

# Check if the connection is successful
if es.ping():
    st.success("Connected to Elasticsearch!")
else:
    st.error("Could not connect to Elasticsearch.")

# Streamlit UI
st.title("Employee Data Indexer")

# Upload CSV file
uploaded_file = st.file_uploader("Upload Employee CSV file", type="csv")
if uploaded_file is not None:
    data = load_employee_data(uploaded_file)
    
    if data.empty:
        st.error("No data loaded. Please check the CSV file.")
    else:
        st.success("Data loaded successfully!")
        st.dataframe(data)  # Display the data

        # Replace NaN values with None
        data = data.where(pd.notnull(data), None)

        # Index the employee data
        if st.button("Index Employee Data"):
            for i, record in data.iterrows():
                doc = record.to_dict()  # Convert each row to a dictionary
                try:
                    # Index the document
                    es.index(index='employees', id=i, document=doc)
                    st.success(f"Indexed document {i + 1}: {doc}")
                except Exception as e:
                    st.error(f"Error indexing document {i}: {e}")

            st.success("All documents indexed successfully!")
