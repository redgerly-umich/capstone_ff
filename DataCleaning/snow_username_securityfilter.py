import pandas as pd 
import numpy as np
import json

def filter_usernames(username, user_dict):

    # Check if the username exists, if not 
    # return null
    try:
        out_val = user_dict[username]['id']
    except:
        out_val = np.nan

    return out_val


def process_usernames():

    # Read in incident data
    df = pd.read_csv('assets/ServiceNow_Incident.csv')

    # Read in username JSON
    with open('assets/User_Name.json') as f:
        username_dict = json.load(f)

    # Apply the user filter
    df['calling_user_id'] = df['calling_user'].apply(lambda x: filter_usernames(x, username_dict)
                                                     if pd.notnull(x) else np.nan)

    # Save file to CSV 
    df.to_csv('assets/ServiceNow_Incident.csv', index=False)

if __name__ == "__main__":
    
    process_usernames()
