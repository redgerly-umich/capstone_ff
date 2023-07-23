import json
import pandas as pd

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

def filter_out_sw_names(df, table, attribs):

    # Get the file name 
    read_file = 'Event_Filter_Attribs/' + table + '_filterAttributes.xlsx'

    # Iterate through the attributes
    for attrib in attribs:

        # read the file 
        unique_names = pd.read_excel(read_file, sheet_name=attrib)

        # Filter down the dataframe 
        df = df[df[attrib].isin(unique_names['Values'])]

    return df

def apply_security_filter(table, attr_df, metadata_df):

    # Read the table from parquet
    current_df = pd.read_parquet('assets/' + table + '.parquet')

    # Filter attribute and metadata df for filtering instructions
    table_info = metadata_df[metadata_df['TableName'] == table]
    attr_info = attr_df[attr_df['Historical_CM'] == table]

    # get the sw column values 
    filter_attr = table_info['Filter out security software on'].iloc[0]
    attribs = [col.strip() for col in filter_attr.split(', ')]

    # filter out sensitive security software 
    current_df = filter_out_sw_names(current_df, table, attribs)

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


# Define a function to get data and export to parquet
def initiate_security_processing(table):

    # Read in data 
    metadata_df = pd.read_excel('Historic_ConfigManager_Tables_Summary.xlsx', sheet_name='SensitiveSecurityInfo')
    attributes_df = pd.read_excel('Historic_ConfigManager_Tables_Summary.xlsx', sheet_name='Attributes_to_Alias')

    # Define the name to save the file
    file_save_name = table.split('.')

    # Apply security filtering and get data
    filtered_df = apply_security_filter(table, attributes_df, metadata_df)

    # Edit the current save name
    save_name = '_'.join(file_save_name) + '.parquet'

    # Send file to parquet 
    filtered_df.to_parquet('assets/' + save_name, engine='pyarrow')

if __name__ == "__main__":

    # Call the function
    for val in ['AppHang_Events_PARSED', 'WindowsError_Events_PARSED', 'AppError_Events_PARSED', 'Boot_Events_PARSED']:
        initiate_security_processing(val)