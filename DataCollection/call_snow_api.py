# Import the necessary modules
import requests
import json
import pandas as pd
import numpy as np
import time

def make_API_call(creds, headers, inst_str, sysparm_limit, sysparm_offset=0):
    '''
    Makes a call to the incident endpoint of the 
    service now API and returns sysparm_limit number of records.
    '''

    # Define the endpoint to pull data
    instance = inst_str + f"&sysparm_limit={str(sysparm_limit)}&sysparm_offset={str(sysparm_offset)}"

    # Do the HTTP request
    response = requests.get(instance, auth=creds, headers=headers, verify=False)

    # If error, send notification
    if response.status_code != 200:
        print('Status:', response.status_code, 'Headers:', response.headers, 'Error Response:', response.json())
        return None
    
    # Otherwise output data
    else:

        # Grab JSON data in dict format
        data = json.loads(response.text)

        # Results key contains a list of dict, grab only dict objects
        load_data = [item for item in data['result'] if type(item) == type(dict())]

        # Append to dataframe
        df = pd.DataFrame(load_data)

        return df

def get_machine_id_API_call(creds, headers, inst_str):
    '''
    Makes a call to the cm endpoint of the 
    service now API and get the machine name for inc tickets.
    '''

    # Do the HTTP request
    response = requests.get(inst_str, auth=creds, headers=headers, verify=False)

    # If error, send notification
    if response.status_code != 200:
        print('Status:', response.status_code, 'Headers:', response.headers, 'Error Response:', response.json())
        return np.nan
    
    # Otherwise output data
    else:

        # Grab JSON data in dict format
        data = json.loads(response.text)

        # Get the configuration item
        return data['result']['name']

def get_records(creds, headers, instance):
    '''
    Call the API to get data from 2023-01-01 
    to as close to the present as possible.
    '''

    # define an output dataframe to accumulate data 
    out_df = pd.DataFrame()

    # Counter for sys_param_offset
    counter = 0

    # Define the number of records to grab for each call
    num_recs_to_grab = 1000

    # Try making 50,000 calls 
    for i in range(50000):

        # Make the API call 
        current_dat = make_API_call(creds, headers, instance, num_recs_to_grab, counter)
        if len(current_dat) < 1:
            print('Retrieval completed')
            break
        counter += num_recs_to_grab

        # Append the result to out_df 
        out_df = pd.concat([out_df, current_dat], axis=0)

        # Sleep to prevent exceeding rate limits
        time.sleep(2)

    return out_df


def get_all_data():

    # Get the credentials
    with open("assets/creds.json", 'r') as f:
        creds = json.load(f)

    # Get endpoint to call
    endpoint = creds['endpoint']

    # read creds
    user = creds['uid']
    password = creds['pwd']
    secrets = (user, password)

    # Set proper headers
    headers = {"Content-Type": "application/json", "Accept": "application/json"}

    # Define the incident and problem instances to pull
    instance_incident = endpoint + "/api/now/table/incident?sysparm_query=sys_created_on>=javascript:gs.dateGenerate('2023-01-01','00:00:00')"
    instance_problem = endpoint + "/api/now/table/problem?sysparm_query=sys_created_on>=javascript:gs.dateGenerate('2023-01-01','00:00:00')"

    # Get INC and problem data 
    df_inc = get_records(secrets, headers, instance_incident)
    df_problem = get_records(secrets, headers, instance_problem)

    # Get machine name from the INC data
    df_inc['configuration_item'] = df_inc['cmdb_ci'].apply(lambda x: get_machine_id_API_call(secrets, headers, eval(x)['link'])
                                                            if pd.notnull(x) else np.nan)
    
    # Save outputs to csv
    df_inc.to_csv('ServiceNow_Incident.csv', index=False)
    df_problem.to_csv('ServiceNow_Problem.csv', index=False)

if __name__ == '__main__':
    
    get_all_data()