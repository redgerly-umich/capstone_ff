import os
import json
import pandas as pd

def get_table_attr_alias(attr_df):

    # Filter for the non-shared attributes
    df = attr_df[attr_df['Attribute shared with other tables?'] != 'Y']

    # Iterate through each table
    for table in df['Historical_CM'].unique():

        folder_name = '_'.join(table.split('.'))

        # Make a directory for the table
        os.mkdir('table_attributes/'+ folder_name)

        # Filter the dataframe for the table
        current_tbl = df[df['Historical_CM']==table]

        # Skip empty attributes
        if len(current_tbl) < 1:
            continue

        # Read in the table 
        current_df = pd.read_parquet('assets/'+ table + '.parquet')

        # Run the table for each attribute
        for attr in current_tbl['Attribute']:

            # Get unique items from the table
            unique_items = current_df[attr].unique()
            
            # Define an output dataframe for ID mapping 
            output_df = pd.DataFrame({"Value": list(unique_items), 
                                    "id": list(range(1, len(unique_items)+1))})
        
            output_df = output_df.set_index("Value")

            # Write each dataframe to a different json.
            output_df.to_json('table_attributes/'+ folder_name + '/' + attr + '.json', orient='index')

def handle_existing_file(existing_file, unique_items):

    # Read shared JSON file
    with open(existing_file, 'r') as f:
        current_dict = json.load(f)
    
    # Get max ID value
    sorted_dict = {k: v for k, v in sorted(current_dict.items(), key=lambda item: item[1]['id'], reverse=True)}
    max_val = list(sorted_dict.items())[0][1]['id']

    # Get unique items that do not exist in the shared JSON file
    new_items = [item for item in unique_items if item not in sorted_dict.keys()]

    # add new items to the dictionary
    for item in new_items:
        max_val +=1
        sorted_dict[item] = {'id': max_val}

    # Save dict to JSON
    with open(existing_file, 'w') as f:
        json.dump(sorted_dict, f)

def get_shared_attr_alias(attr_df):

    # Filter for the shared attributes
    shared = attr_df[attr_df['Attribute shared with other tables?'] == 'Y']

    for attr in shared['clean_attr'].unique():

        # Get the tables with the shared attribute
        current = shared[shared['clean_attr'] == attr]

        # Define a set for unique items
        unique_items = set()

        # Get distinct items
        for table in current['Historical_CM']:

            current_table = current[current['Historical_CM'] == table]

            # Read in the table 
            current_df = pd.read_parquet('assets/'+ table + '.parquet')
            
            # Call functions to get distict values for the current attribute
            distinct_items = current_df[current_table['Attribute'].iloc[0]].unique()

            # Union with the set of items for the current attribute
            unique_items = set(distinct_items) | unique_items

        out_values = list(unique_items)

        # Check if the current attr json exists
        test_exists_file = 'shared_attributes/' + attr + '.json'
        if os.path.isfile(test_exists_file):
            handle_existing_file(test_exists_file, out_values)

        # Otherwise, write a new attribute JSON file
        else:
            # Define an output dataframe for ID mapping 
            output_df = pd.DataFrame({"Value": out_values, 
                                    "id": list(range(1, len(out_values)+1))})
            
            output_df = output_df.set_index("Value")
            
            # Write each dataframe to JSON
            output_df.to_json('shared_attributes/' + attr + '.json', orient='index')

if __name__ == "__main__":

    # Read in the attributes dataframe
    attributes_df = pd.read_excel('Historic_ConfigManager_Tables_Summary.xlsx', sheet_name='Attributes_to_Alias')

    # Filter down attributes df for the event data
    attributes_df = attributes_df[attributes_df['Historical_CM'].isin(['AppHang_Events_PARSED', 'WindowsError_Events_PARSED',
                                                                        'AppError_Events_PARSED', 'Boot_Events_PARSED'])]
    # Call the function
    get_table_attr_alias(attributes_df)
    get_shared_attr_alias(attributes_df)