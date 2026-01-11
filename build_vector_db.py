import json
from langchain_core.documents import Document
from langchain_community.embeddings import HuggingFaceEmbeddings
from langchain_community.vectorstores import FAISS


#1. loading the schema.
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
   
   
# 2. summarize:
def schema_summary(schema):
    """
    Create text summaries for all tables in the schema.
    
    Args:
        schema (dict): Database schema dictionary
    
    Returns:
        list: List of summary strings for all tables
    """
    
    summaries = []
    
    for table_name, table_info in schema.items():
        columns = table_info['columns']
        primary_keys = table_info['primary_keys']
        foreign_keys = table_info['foreign_keys']
        
        # Build column names list
        column_names = list(columns.keys())
        columns_text = ", ".join(column_names)
        
        # Base summary
        summary = f"This table {table_name} , has columns {columns_text}."
        
        # Add primary key info if exists
        if primary_keys:
            pk_text = ", ".join(primary_keys)
            summary += f" The primary key in this table is {pk_text}."
        
        # Add foreign key info if exists
        if foreign_keys:
            fk_descriptions = []
            for fk in foreign_keys:
                fk_desc = f"{fk['column']} references {fk['references_table']}.{fk['references_column']}"
                fk_descriptions.append(fk_desc)
            fk_text = "; ".join(fk_descriptions)
            summary += f" Foreign keys: {fk_text}."
        
        summaries.append(summary)
    
    return summaries

# 3. create documents for each summary.
def create_documents(summaries, schema):
    """
    Create Document objects from schema summaries.
    
    Args:
        summaries (list): List of summary strings for all tables
        schema (dict): Database schema dictionary
    
    Returns:
        list: List of Document objects with page_content and metadata
    """
    documents = []
    table_names = list(schema.keys())
    
    for i, summary in enumerate(summaries):
        doc = Document(
            page_content=summary,
            metadata={'table_name': table_names[i]}
        )
        documents.append(doc)
    
    return documents

# 4. embedding the summary and storing it to a vector db
def create_vector_db(documents, save_path='faiss_vector'):
    """
    Create and save a FAISS vector database from documents.
    
    Args:
        documents (list): List of Document objects
        save_path (str): Path to save the FAISS vector database. Defaults to 'faiss_vector'
    
    Returns:
        FAISS: The created vector store
    """
    # Initialize embeddings
    embeddings = HuggingFaceEmbeddings(
        model_name="sentence-transformers/all-MiniLM-L6-v2")
    
    # Create vector store from documents
    vectorstore = FAISS.from_documents(
        documents=documents,
        embedding=embeddings)
    
    # Save the vector store to disk
    vectorstore.save_local(save_path)
    print(f"Vector database saved to '{save_path}' folder")
    
    return vectorstore

def main():
    #1. Load the schema
    schema = load_schema()
    
     #2. Create summaries for all tables
    if schema:
        summaries = schema_summary(schema)
        
        # 3. create documents for each summary.
        documents = create_documents(summaries, schema)
        
        # 4. embedding the summary and storing it to a vector db
        vectorstore = create_vector_db(documents)
        print("Vector database created successfully!")


if __name__ == "__main__":
    main()