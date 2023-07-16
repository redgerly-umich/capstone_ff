# Load modules
import pandas as pd 
import xml.etree.ElementTree as ET

# Set the notebook to display all columns of a dataframe
pd.set_option('display.max_columns', None)

def parse_xml(xml_str, in_dict):

    # Decode the XML
    xml_str = xml_str.replace(' xmlns="http://schemas.microsoft.com/win/2004/08/events/event"','').replace('&lt;', '<').replace('&gt;', '>').replace('<![CDATA[', '').replace(']]>', '')

    out_dict = {}

    root = ET.fromstring(xml_str)
    
    for key, val in in_dict.items():

        try:
            out_dict[key] = root[0][val].text
        
        except:
            out_dict[key] = 'Not available'

    return out_dict
    
def expand_dict_values(in_df, in_dict):
    
    # Parse the XML 
    in_df['dict_EventData'] = in_df['EventDataXML'].apply(lambda x: parse_xml(x, in_dict))

    # only get fields that are non-null
    expanded_fields = in_df['dict_EventData'][in_df['dict_EventData'].notnull()]

    # expand attributes
    expanded_fields = expanded_fields.apply(pd.Series)
        
    # Join with remaining data      
    result = pd.merge(in_df, expanded_fields, 
                        how = 'left', right_index = True, left_index=True)

    return result

def process_win_errors_file(infile, outfile):

    # Read in the excel file
    df = pd.read_excel(infile)

    # Denote positions of the data in the XML string
    windows_error_dict = {"Data_1": 0,
                        "FaultBucketType": 1, 
                        "EventName": 2, 
                        "Response": 3,
                        "CabID": 4,
                        "ProblemSignatureP1_Application": 5,
                        "ProblemSignatureP2_AppVersion": 6,
                        "ProblemSignatureP3": 7,
                        "ProblemSignatureP4_Module": 8,
                        "ProblemSignatureP5_ModuleVersion": 9,
                        "ProblemSignatureP6": 10,
                        "ProblemSignatureP7_ExceptionCode": 11,
                        "ProblemSignatureP8": 12,
                        "ProblemSignatureP9": 13, 
                        "ProblemSignatureP10": 14,
                        "AttachedFiles": 15,
                        "AttachedFilesPath": 16,
                        "AnalysisSymbol": 17,
                        "RecheckingForSolution": 18,
                        "ReportID": 19,
                        "HashedBucket": 20,
                        "CabGuid": 21
                        }
    
    # Call the data processing function 
    out_df = expand_dict_values(df, windows_error_dict)

    # drop unneeeded columns
    dont_save = ['EventDataXML', 'dict_EventData']
    out_df = out_df[[col for col in out_df.columns if col not in dont_save]]

    # Save result to CSV
    out_df.to_csv(outfile)

    return out_df


if __name__ == '__main__':

    df = process_win_errors_file("WindowsError_Events_Sample.xlsx", "WindowsError_Events_PARSED.csv")