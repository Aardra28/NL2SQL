import pymysql
import json
import os

def get_database_schema(db_config):
    # Connect to database
    connections = pymysql.connect(**db_config)
    cursor = connections.cursor()
    
    # Get all tables
    cursor.execute('show tables')
    tables = cursor.fetchall()
    
    # Dictionary to store all table information
    schema = {}
    
    # Loop through each table and describe it
    for table in tables:
        table_name = table[0]  # Extract table name from tuple
        
        # Get column information
        cursor.execute(f'DESCRIBE {table_name}')
        table_des = cursor.fetchall()
        
        columns = {}
        primary_keys = []
        
        for col in table_des:
            field, type_, null, key, default, extra = col
            
            # Add column name and type to columns dict
            columns[field] = type_
            
            # Check if it's a primary key
            if key == 'PRI':
                primary_keys.append(field)
        
        # Get foreign key information
        fk_query = f"""
        SELECT 
            COLUMN_NAME,
            REFERENCED_TABLE_NAME,
            REFERENCED_COLUMN_NAME
        FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
        WHERE TABLE_SCHEMA = '{db_config['database']}'
            AND TABLE_NAME = '{table_name}'
            AND CONSTRAINT_NAME LIKE '%_ibfk_%'
        """
        
        cursor.execute(fk_query)
        foreign_keys_info = cursor.fetchall()
        
        # Process foreign keys
        foreign_keys = []
        for fk in foreign_keys_info:
            column_name, ref_table, ref_column = fk
            foreign_keys.append({
                'column': column_name,
                'references_table': ref_table,
                'references_column': ref_column
            })
        
        # Store table information
        schema[table_name] = {
            'columns': columns,
            'primary_keys': primary_keys,
            'foreign_keys': foreign_keys
        }
    
    # Close connections
    cursor.close()
    connections.close()
    
    # Convert to JSON and return
    return json.dumps(schema, indent=4)


def main():
    db_config = {
        'database': 'hms',
        'host': 'localhost',
        'user': 'root',
        'password': 'aardra'
    }
    
    # Get schema as JSON
    schema_json = get_database_schema(db_config)
    
    # Print the JSON
    print(schema_json)
    
    # Get the directory where this script is located
    script_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Create the full path for the JSON file
    json_file_path = os.path.join(script_dir, 'database_schema.json')
    
    # Save to file in the same directory as the script
    with open(json_file_path, 'w') as json_file:
        json_file.write(schema_json)
    
    print(f"\nSchema saved to {json_file_path}")


if __name__ == "__main__":
    main()