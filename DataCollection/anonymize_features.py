import pandas as pd 
import json
import pyodbc
import os

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

def get_table_attr_alias(conn_obj, attr_df):

    # Filter for the non-shared attributes
    df = attr_df[attr_df['Attribute shared with other tables?'] != 'Y']

    # Iterate through each table
    for table in df['Historical_CM'].unique():

        folder_name = '_'.join(table.split('.'))

        # Make a directory for the table
        os.mkdir('table_attributes/'+ folder_name)

        # Filter the dataframe for the table
        current_tbl = df[df['Historical_CM']==table]

        # Run the SQL for each attribute
        for attr, sql_query in zip(current_tbl['Attribute'], current_tbl['SQL']):

            unique_items = get_persist_sampleData(conn_obj, sql_query)

            # Define an output dataframe for ID mapping 
            output_df = pd.DataFrame({"Value": list(unique_items.iloc[:,0]), 
                                    "id": list(range(1, len(unique_items)+1))})
        
            output_df = output_df.set_index("Value")

            # Write each dataframe to a different json.
            output_df.to_json('table_attributes/'+ folder_name + '/' + attr + '.json', orient='index')

def get_shared_attr_alias(conn_obj, attr_df):

    # Filter for the shared attributes
    shared = attr_df[attr_df['Attribute shared with other tables?'] == 'Y']

    for attr in shared['clean_attr'].unique():

        # Get the tables with the shared attribute
        current = shared[shared['clean_attr'] == attr]

        # Define a set for unique items
        unique_items = set()

        # Execute sql for distinct items 
        for sql_query in current['SQL']:
            
            # Call functions to get tables in database
            distinct_items = get_persist_sampleData(conn_obj, sql_query)

            # Append to unique_df
            unique_items = set(distinct_items.iloc[:,0]) | unique_items

            if attr == 'User_Name':
                out_values = list(set(unique_items))
            else:
                out_values = list(unique_items)

        # Define an output dataframe for ID mapping 
        output_df = pd.DataFrame({"Value": out_values, 
                                 "id": list(range(1, len(out_values)+1))})
        
        output_df = output_df.set_index("Value")
        
        # Write each dataframe to a different worksheet.
        output_df.to_json('shared_attributes/' + attr + '.json', orient='index')

if __name__ == '__main__':

    # Import access credentials
    with open('assets/fun.json') as f:
            creds = json.load(f)

    # Connect to database 
    dbconfig_conn = connect_to_database(creds["on_prem_server"], creds["on_prem_db"])

    # Read in the attributes dataframe
    attributes_df = pd.read_excel('Historic_ConfigManager_Tables_Summary.xlsx', sheet_name='Attributes_to_Alias')

    # Call the function
    get_table_attr_alias(dbconfig_conn, attributes_df)

    # Call the function 
    get_shared_attr_alias(dbconfig_conn, attributes_df)
