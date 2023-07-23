# Import Modules
import pandas as pd 
import pyarrow.parquet as pq
import pyodbc
import json

# Function Definitions ---------------------------

def connect_to_database(server, database):
    ''' 
    Uses pyodbc to connect to a SQL Server database
    and returns the connection object
    '''
    # Define database driver, server, database
    driver = 'SQL SERVER'

    # Define the connection string
    conn_str = 'DRIVER={' + driver + '};SERVER=' + server + ';DATABASE=' + database + ';Trusted_Connection=yes;'

    # Connect to database using windows authentication 
    conn = pyodbc.connect(conn_str)

    return conn 

def get_persist_sampleData(persist_conn_obj, sql_query):
    ''' 
    Gets rows for a persisted table in the database connection
    '''
    # Execute a query for persisted table
    df_pers_all = pd.read_sql_query(sql_query, persist_conn_obj)
    
    return df_pers_all


def get_query_chunking(table, machine_col, id_column, start, end):
        # Define a SQL query
        sql_query = f""" 
                SELECT
                * 
                FROM 
                {table}
                WHERE
                {machine_col} IN (

                SELECT DISTINCT 
                ItemKey 
                FROM 
                persist.System_DISC 
                WHERE 
                Netbios_Name0 LIKE '%[A-Z]-[A-Z]-[A-Z0-9]%' 
                OR Netbios_Name0 LIKE 'ENDPOINT%'
                ) AND 
                {id_column} BETWEEN {start} AND {end}
                AND RWB_EFFECTIVE_DATE <= '2023-07-04'
                """
        return sql_query

def get_query_no_chunk(table, machine_col):
        # Define a SQL query
        sql_query = f""" 
                SELECT
                * 
                FROM 
                {table} 
                WHERE
                {machine_col} IN (

                SELECT DISTINCT 
                ItemKey 
                FROM 
                persist.System_DISC 
                WHERE 
                Netbios_Name0 LIKE '%[A-Z]-[A-Z]-[A-Z0-9]%' 
                OR Netbios_Name0 LIKE 'ENDPOINT%'
                )
                """
        return sql_query

def get_query_no_machid(table):
        # Define a SQL query
        sql_query = f""" 
                SELECT
                * 
                FROM 
                {table} 
                WHERE 
                RWB_EFFECTIVE_DATE <= '2023-07-04'
                """
        return sql_query

def deidentify(current_dict, value):
    '''
    Uses a dict-like object of attribute-identifier
    mappings to return an id for inputted attribute.
    '''

    try: 
        # Test if the input is None
        if str(value) == "None":
            return current_dict["None"]['id']
        
        else:
            return current_dict[value]['id']
    
    except:
        return current_dict[str(value)]['id']

def scan_id_cols(in_dict, in_series):
    ''' 
    Scans the column that needs to be 
    aliased with a unique identifier and 
    adds objects that are currently not 
    in the input dictionary
    '''
    
    # Get max value
    sorted_dict = {k: v for k, v in sorted(in_dict.items(), key=lambda item: item[1]['id'], reverse=True)}
    max_val = list(sorted_dict.items())[0][1]['id']

    # Return an id that notifys program of missing item
    out_flag = False

    # Scan through series
    for obj in in_series:
        
        # Add item if it is not in the dict
        if obj not in in_dict.keys():
            max_val += 1
            in_dict[obj] ={'id': max_val}
            out_flag = True

    return in_dict, out_flag

def apply_security_filter(conn_obj, table, query, attr_df, metadata_df):
    '''
    Call a query to the database and apply 
    security filtering to it.
    '''
     
    # Call functions to get tables in database
    current_df = get_persist_sampleData(conn_obj, query)

    # Filter attribute and metadata df for filtering instructions
    table_info = metadata_df[metadata_df['TableName'] == table]
    attr_info = attr_df[attr_df['Historical_CM'] == table]

    # First, remove columns 
    remove_col_str = table_info['Remove'].iloc[0]
    if pd.notnull(remove_col_str):
        
        cols_to_remove = [col.strip() for col in remove_col_str.split(', ')]

        # Remove columns   
        current_df = current_df[[col for col in current_df if col not in cols_to_remove]]

    # Get shared attributes
    shared = attr_info[attr_info['Attribute shared with other tables?'] == 'Y']
    not_shared = attr_info[attr_info['Attribute shared with other tables?'] != 'Y'] 

    if len(shared) >=1:
        for attr, clean_attr in zip(shared['Attribute'], shared['clean_attr']):
            
            # Read shared JSON file
            with open('shared_attributes/' + clean_attr + '.json', 'r') as f:
                current_dict = json.load(f)
            
            # Add items that do not currently exist
            current_dict, change = scan_id_cols(current_dict, current_df[attr].unique())

            # Save the result
            if change:
                with open('shared_attributes/' + clean_attr + '.json', 'w') as f:
                    json.dump(current_dict, f)

            # Apply security filter on shared attribute
            try:
                current_df[attr] = current_df[attr].apply(lambda x: deidentify(current_dict, x))
            except:
                current_dict = {str(key).lower(): val for key, val in current_dict.items()}
                current_df[attr] = current_df[attr].apply(lambda x: deidentify(current_dict, str(x).lower()))
    
    if len(not_shared) >=1:
        for attr in not_shared['Attribute']:
            
            directory_name = '_'.join(table.split('.'))

            # Read not-shared JSON
            with open('table_attributes/' + directory_name + '/' + attr + '.json', 'r') as f:
                current_dict = json.load(f)

            # Add items that do not currently exist
            current_dict, change = scan_id_cols(current_dict, current_df[attr].unique())

            # Save the result
            if change:
                with open('table_attributes/' + directory_name + '/' + attr + '.json', 'w') as f:
                    json.dump(current_dict, f)
            
            # Apply security filter on non-shared attribute
            try:
                current_df[attr] = current_df[attr].apply(lambda x: deidentify(current_dict, x))
            except:
                current_dict = {str(key).lower(): val for key, val in current_dict.items()}
                current_df[attr] = current_df[attr].apply(lambda x: deidentify(current_dict, str(x).lower()))

    return current_df

# Function to pull data in chunks from the SQL database
def implement_chunking(conn_obj, table, id_column, mach_id, chunk_size, file_save_name, metadata_df, attr_df):
    ''' 
    Removes sensitive security information for 
    large SQL objects in chunks.
    '''

    query_table_size = f"Select COUNT(*) FROM {table}"
    count = get_persist_sampleData(conn_obj, query_table_size)
    total_size = count.iloc[0,0]

    # Get the number of iterations needed
    num_iters = total_size // chunk_size+1
    start = 0

    for i in range(num_iters):

        # Get the query for the chunk size
        sql_query = get_query_chunking(table, mach_id, id_column, start+1, start+chunk_size)

        # Apply security filtering and get data using the query above
        filtered_chunk = apply_security_filter(conn_obj, table, sql_query, attr_df, metadata_df)
        
        # Edit the current save name
        save_name = '_'.join(file_save_name) + f'_{str(i+1)}' + '.parquet'

        # Send file to parquet 
        filtered_chunk.to_parquet('assets/' + save_name, engine='pyarrow')

        # Add to the counter
        start += chunk_size

# Define a function to get data and export to parquet
def export_sql_object(conn_obj, table, chunk_size):

    # Read in data 
    metadata_df = pd.read_excel('Historic_ConfigManager_Tables_Summary.xlsx', sheet_name='SensitiveSecurityInfo')
    attributes_df = pd.read_excel('Historic_ConfigManager_Tables_Summary.xlsx', sheet_name='Attributes_to_Alias')

    # Filter for the table metadata 
    table_data = metadata_df[metadata_df['TableName'] == table]

    # Get MachineID 
    mach_id = table_data.iloc[0,1]

    # Define the name to save the file
    file_save_name = table.split('.')

    # Determine if chunking is needed
    chunking_var = table_data.iloc[0,2]
    
    if pd.notna(chunking_var):

        # Implement chunking if needed
        implement_chunking(conn_obj, table, chunking_var, mach_id, chunk_size, file_save_name,  metadata_df, attributes_df)

    # Skip chunking if not needed
    else:
        
        # Get the query for the data
        if pd.isna(mach_id):
            sql_query = get_query_no_machid(table)
        else:
            sql_query = get_query_no_chunk(table, mach_id)

        # Apply security filtering and get data
        filtered_df = apply_security_filter(conn_obj, table, sql_query, attributes_df, metadata_df)

        # Edit the current save name
        save_name = '_'.join(file_save_name) + '.parquet'

        # Send file to parquet 
        filtered_df.to_parquet('assets/' + save_name, engine='pyarrow')

if __name__ == "__main__":

    # Import access credentials
    with open('assets/fun.json') as f:
            creds = json.load(f)

    # Connect to database 
    dbconfig_conn = connect_to_database(creds["on_prem_server"], creds["on_prem_db"])

    # Export the data to parquet
    tbl_name = 'Persist.USER_DISC'
    export_sql_object(dbconfig_conn, tbl_name, 5000000)
