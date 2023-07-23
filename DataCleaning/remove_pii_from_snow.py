import pandas as pd 
import re
import spacy 
from spacy.lang.en import English

# Define a function for filtering Service Now Incident data
def filter_inc_data(inc_df, category_df):

    # Remove PII columns 
    remove_cols = ['u_name', 'sys_created_by', 'u_requested_for', 
                    'u_project', 'u_access_id', 'u_point_of_contact_bol',
                     'user_input', 'u_email_address', 'u_point_of_contact',
                     'u_affected_user', 'additional_assignee_list', 'reopened_by',
                     'description', 'comments', 'work_notes', 'close_notes']
    cols = [col for col in inc_df.columns if col not in remove_cols]

    # Phone number regex
    phone_num = '[\d]{3,}+-[\d]{3}-[\d]{4}'

    # Filter out emails
    inc_df['is_email'] = inc_df['short_description'].apply(lambda x: 1 if re.search('@', x) else 0)
    inc_df = inc_df[inc_df['is_email'] == 0]

    # remove PII columns
    inc_df = inc_df[cols]

    # Filter out phone numbers 
    inc_df['is_phone'] = inc_df['short_description'].apply(lambda x: 1 if re.search(phone_num, x) else 0)
    inc_df = inc_df[inc_df['is_phone'] == 0]

    # Filter out client data 
    inc_df = inc_df[inc_df['u_client'] == False]

    # Filter out autosys records
    inc_df = inc_df[~inc_df['short_description'].str.contains('autosys')]
    inc_df = inc_df[~inc_df['short_description'].str.contains('Autosys')]
    inc_df = inc_df[~inc_df['short_description'].str.contains('AutoSys')]

    #Remove irrelevant categories 
    inc_df = inc_df[inc_df['category'].isin(category_df['category'].unique())]
    inc_df = inc_df[inc_df['subcategory'].isin(category_df['subcategory'].unique())]

    return inc_df 

def identify_pii(in_df, field):
    
    nlp = English()

    # Instantiate a Tokenizer for English
    tokenizer = nlp.tokenizer

    # Assign SpaCy `en_core_web_` as `nlp`.
    nlp = spacy.load('en_core_web_trf')
    ner = nlp.get_pipe('ner')

    # Create function to add an article's tokens to `doc_list`.
    # Tokenize one time, then use that object for the subsequent accumulators.
    # Returns None many times.
    doc_list = []
    def to_doc_list(text):
        doc_list.append(nlp(text))

    # Takes time to generate tokens from each cell's fulltext.
    in_df[field].apply(to_doc_list)

    # Assign `doc_list` to `doc_series` as a Series object.
    doc_series = pd.Series(doc_list)

    out_filtered_strings = []
    for doc in doc_series:
        filtered_string = ""
        for token in doc:
            if token.ent_type_ in ['PERSON', 'MONEY', 'CARDINAL', 'QUANTITY', 'PERCENT']:
                new_token = " <{}>".format(token.ent_type_)
            elif token.pos_ == "PUNCT":
                new_token = token.text
            else:
                new_token = " {}".format(token.text)
            filtered_string += new_token
        filtered_string = filtered_string[1:]
        out_filtered_strings.append(filtered_string)

    # add new column in the in_df
    in_df[field] = out_filtered_strings

    return in_df

if __name__ == '__main__':

    # Read in Service Now Incident data 
    df_inc = pd.read_excel('assets/FILTERED_ServiceNow_Incidents_Sample_20230710.xlsx', sheet_name = 'Sheet1')

    # Read in target Service Now Incident categories 
    cat_df = pd.read_excel('assets/FILTERED_ServiceNow_Incidents_Sample_20230710.xlsx', sheet_name = 'Sheet2')

    # Get filtered inc dataset
    df_inc_filtered = filter_inc_data(df_inc, cat_df)

    # Run NER on short_description
    out_df = identify_pii(df_inc_filtered, 'short_description')
    out_df.to_csv('ServiceNow_Incident.csv', index=False)