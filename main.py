from langchain_community.embeddings import HuggingFaceEmbeddings
from langchain_community.vectorstores import FAISS
import json
import pymysql

def load_db(load_path='faiss_vector'):
    """
    Load the FAISS vector database from disk.
    
    Args:
        load_path (str): Path to load the FAISS vector database from. Defaults to 'faiss_vector'
    
    Returns:
        FAISS: The loaded vector store
    """
    # Initialize embeddings (same model used during creation)
    embeddings = HuggingFaceEmbeddings(
        model_name="sentence-transformers/all-MiniLM-L6-v2"
    )
    
    # Load the vector store from disk
    vectorstore = FAISS.load_local(
        load_path, 
        embeddings,
        allow_dangerous_deserialization=True
    )
    
    print(f"Vector database loaded from '{load_path}' folder")
    
    return vectorstore


def load_schema(file_path='database_schema.json'):
    """
    Loads the database schema from a JSON file.
    
    Args:
        file_path (str): Path to the JSON schema file. Defaults to 'database_schema.json'
    
    Returns:
        dict: Dictionary containing the database schema
    """
    try:
        with open(file_path, 'r') as json_file:
            schema = json.load(json_file)
        return schema
    except FileNotFoundError:
        print(f"Error: File '{file_path}' not found.")
        return None
    except json.JSONDecodeError:
        print(f"Error: Invalid JSON in file '{file_path}'.")
        return None


def get_relevant_schemas(question, vectorstore, schema, top_k=3):
    """
    Retrieve relevant table schemas based on the user question.
    
    Args:
        question (str): User's query
        vectorstore (FAISS): Loaded vector database
        schema (dict): Complete database schema
        top_k (int): Number of top relevant tables to retrieve. Defaults to 3
    
    Returns:
        dict: Dictionary containing only the relevant table schemas
    """
    # Search for similar documents with scores
    results_with_scores = vectorstore.similarity_search_with_score(question, k=top_k)
    
    # Extract relevant table names
    relevant_tables = []
    print(f"Top {top_k} relevant tables for query: '{question}'\n")
    
    for i, (doc, score) in enumerate(results_with_scores, 1):
        table_name = doc.metadata['table_name']
        relevant_tables.append(table_name)
        print(f"{i}. {table_name} (Score: {score:.4f})")
        print(f"   Summary: {doc.page_content}")
        print()
    
    print(f"📋 Relevant tables: {relevant_tables}\n")
    
    # Get schemas for relevant tables only
    selected_tables_schema = {}
    for table_name in relevant_tables:
        if table_name in schema:
            selected_tables_schema[table_name] = schema[table_name]
    
    return selected_tables_schema


import json
import os
from groq import Groq
from dotenv import load_dotenv

def llm_sql_query(question, selected_tables_schema):
    """
    Generate SQL query using LLM based on user question and relevant table schemas.
    
    Args:
        question (str): User's query
        selected_tables_schema (dict): Dictionary containing relevant table schemas
    
    Returns:
        str: Generated SQL query
    """
    # Load environment variables
    load_dotenv()
    
    # Get API key from .env file
    api_key = os.getenv("GROQ_API_KEY")
    
    # Initialize Groq client
    client = Groq(api_key=api_key)
    
    # Create prompt
    prompt = f"""You are a SQL expert. Generate a MySQL query based on the user's question and the provided database schema.

Database Schema:
{json.dumps(selected_tables_schema, indent=2)}

User Question: {question}

Generate a valid MySQL query that answers the user's question. Return ONLY a JSON object with the following format:
{{
  "sql_query": "YOUR SQL QUERY HERE"
}}

Do not include any other text, markdown, or code blocks. Return only valid JSON."""
    
    # Call LLM
    completion = client.chat.completions.create(
        model="meta-llama/Llama-4-Maverick-17B-128E-Instruct",
        messages=[
            {
                "role": "system",
                "content": "You are a SQL expert. Generate valid MySQL queries based on the given schema and user questions. Always return valid JSON format."
            },
            {
                "role": "user",
                "content": prompt
            }
        ],
        temperature=0.1,  
        max_completion_tokens=1024,
        top_p=1,
        response_format={"type": "json_object"}, 
        stream=False
    )
    
    # Parse response
    response_text = completion.choices[0].message.content
    sql_result = json.loads(response_text)
    sql_query = sql_result.get('sql_query')
    
    print(f"Generated SQL Query: {sql_query}")
    
    return sql_query

def sql_response(sql_query, db_config):
    """
    Execute SQL query and fetch the results.
    
    Args:
        sql_query (str): SQL query to execute
        db_config (dict): Database configuration
    
    Returns:
        list: List of tuples containing query results
    """
    try:
        # Connect to database
        connection = pymysql.connect(**db_config)
        cursor = connection.cursor()
        
        # Execute the query
        cursor.execute(sql_query)
        results = cursor.fetchall()
        
        # Close connection
        cursor.close()
        connection.close()
        
        print(f"\nQuery executed successfully!")
        print(f"Number of rows returned: {len(results)}")
        
        return results
        
    except Exception as e:
        print(f"Error executing query: {e}")
        return None
   
import streamlit as st


import streamlit as st

def streamlit_ui():
    """
    Streamlit UI for SQL RAG application
    """
    # Page configuration
    st.set_page_config(
        page_title="SQL RAG - Natural Language to SQL",
        page_icon="🗄️",
        layout="wide"
    )
    
    # Title and description
    st.title("🗄️ SQL RAG: Natural Language to SQL Query")
    st.markdown("Ask questions about your database in natural language!")
    
    # Database configuration (hidden from UI)
    db_config = {
        'database': 'hms',
        'host': 'localhost',
        'user': 'root',
        'password': 'aardra'
    }
    
    # Initialize session state and load resources automatically
    if 'vectorstore' not in st.session_state:
        st.session_state.vectorstore = load_db()
    
    if 'schema' not in st.session_state:
        st.session_state.schema = load_schema()
    
    # User question input
    st.subheader("❓ Ask Your Question")
    question = st.text_area(
        "Enter your question:",
        placeholder="e.g., Give me all the appointments made by patient 2",
        height=100
    )
    
    # Query button
    if st.button("🚀 Generate SQL Query", type="primary"):
        if not question:
            st.warning("⚠️ Please enter a question")
            return
        
        # Get relevant schemas (with default top_k=3)
        with st.spinner("Processing your question..."):
            relevant_schemas = get_relevant_schemas(
                question, 
                st.session_state.vectorstore, 
                st.session_state.schema, 
                top_k=3
            )
        
        # Display relevant tables
        with st.expander("📋 Relevant Tables Found", expanded=True):
            for table_name in relevant_schemas.keys():
                st.write(f"- **{table_name}**")
        
        # Generate SQL query
        with st.spinner("Generating SQL query..."):
            try:
                sql_query = llm_sql_query(question, relevant_schemas)
            except Exception as e:
                st.error(f"❌ Error generating SQL query: {e}")
                return
        
        # Display SQL query
        st.subheader("📝 Generated SQL Query")
        st.code(sql_query, language="sql")
        
        # Execute query
        with st.spinner("Fetching results from database..."):
            try:
                results = sql_response(sql_query, db_config)
            except Exception as e:
                st.error(f"❌ Error executing query: {e}")
                return
        
        # Display results
        if results:
            st.success(f"✅ Query executed successfully! Found {len(results)} row(s)")
            
            st.subheader("📊 Query Results")
            
            # Create a nice table display
            if len(results) > 0:
                # Convert to a more readable format
                import pandas as pd
                
                # Try to create DataFrame if possible
                try:
                    df = pd.DataFrame(results)
                    st.dataframe(df, use_container_width=True)
                except:
                    # Fallback to displaying as is
                    for i, row in enumerate(results, 1):
                        st.write(f"**Row {i}:** {row}")
            else:
                st.info("No results found")
        else:
            st.warning("⚠️ No results returned from the query")
    
    # Footer
    st.divider()
    st.markdown("---")
    st.caption("💡 Tip: Try asking questions like 'Show all patients', 'List appointments for doctor 5', etc.")


def main():
    streamlit_ui()


if __name__ == "__main__":
    main()