# Load modules
import pandas as pd
import pyarrow.parquet as pq 
import xml.etree.ElementTree as ET
import html
import time

# Set the notebook to display all columns of a dataframe
pd.set_option('display.max_columns', None)

def read_parquet(parquet_file, provider_name):

    # Read in the parquet file
    parquet_data = pq.ParquetFile(parquet_file)

    # Read parquet data in chunks 
    for chunk in parquet_data.iter_batches(10000):

        parquet_df = chunk.to_pandas()

        if provider_name != 'Boot':

            filtered_df = parquet_df[~parquet_df['EventID'].isin([20004, 20098, 20099, 20398, 20399,
            20400, 20498, 65279, 30100])]

            filtered_df = filtered_df[filtered_df['ProviderName'] == provider_name]

            yield filtered_df
        
        else:
            yield parquet_df

    # Close parquet file 
    #parquet_data.close()


def parse_winerror_xml(xml_str, in_dict):

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

def parse_boot_xml(xml_str):
    
    # Decode the xml
    cleaned_xml = xml_str.replace(' xmlns="http://schemas.microsoft.com/win/2004/08/events/event"','').replace('&lt;', '<').replace('&gt;', '>').replace('<![CDATA[', '').replace(']]>', '')

    #Blank dict to add data
    out_dict = {}

    # Parse the xml 
    root = ET.fromstring(cleaned_xml)

    # Grab the power off time
    for val in root.find('.//BootReason'):
        if val.tag == 'EventData' and 'SystemPowerOffTime' in val.attrib:
            out_dict['SystemPowerOffTime'] = val.attrib['SystemPowerOffTime']
            break

    # Grab the EventData
    attrs = ['MachineName', 'AppVersion', 'Reason', 'ExceptionCode', 'param5', 'ActionDescription', 'SystemPath']
    for i, val in enumerate(root.find('.//EventData')):
        out_dict[attrs[i]] = val.text
        if i == 6:
            break
            
    # Grab Custom Data
    try:
        for val in root.find('.//CustomData'):
            out_dict[val.tag] = val.text
    except:
        pass

    return out_dict

def xml_to_col(xml_str, provider_name):

    # Extract CDATA section
    unescaped_string = html.unescape(xml_str)

    # Extract CDATA section
    start = unescaped_string.find('<![CDATA[') + len('<![CDATA[')
    end = unescaped_string.find(']]>')
    cdata = unescaped_string[start:end]

    # Wrap the CDATA content with a root element
    wrapped_cdata = '<root>' + cdata + '</root>'

    # Parse the wrapped CDATA as XML
    root = ET.fromstring(wrapped_cdata)

    if provider_name == 'Application Error':
        row_dict = {
            'FaultingApplicationName':'',
            'AppVersion': '',
            'AppTimestamp': '',
            'FaultingModuleName': '',
            'ModuleVersion': '',
            'ModuleTimestamp': '',
            'ExceptionCode': '',
            'FaultOffset': '',
            'FaultingProcessId': '',
            'FaultingApplicationStartTime': '',
            'FaultingApplicationPath': '',
            'FaultingApplicationModulePath': '',
            'ReportId': '',
            'FaultingPackageFullName': '',
            'FaultingPackageRelativeApplicationID': '',
            'FullPath': '',
            'ProductVersion': '',
            'ProductName': '',
            'Publisher': '',
            'ProgramId': '',
            'FileId': '',
            'FileVersion': ''
        }
    elif provider_name == 'Application Hang':
        row_dict = {
            'Program':'',
            'ProgramVersion': '',
            'ProcessID': '',
            'StartTime': '',
            'TerminationType': '',
            'ApplicationPath': '',
            'ReportID': '',
            'FaultingPackageFullName': '',
            'FaultingPackageRelativeApplicationID': '',
            'HangType': '',
            'BinaryValue': '',
            'FullPath': '',
            'ProductVersion': '',
            'ProductName': '',
            'Publisher': '',
            'ProgramId': '',
            'FileId': '',
            'FileVersion': '',
        }

    for i in range(len(root[0])):
        row_dict[list(row_dict.items())[i][0]] = root[0][i].text
    for i in range(len(root[1])):
        if i < 7:
            row_dict[list(row_dict.items())[int(i + len(root[0]))][0]] = root[1][i].text
    return row_dict
    
def expand_dict_values(in_df):

    # only get fields that are non-null
    expanded_fields = in_df['dict_EventData'][in_df['dict_EventData'].notnull()]

    # expand attributes
    expanded_fields = expanded_fields.apply(pd.Series)
        
    # Join with remaining data      
    result = pd.merge(in_df, expanded_fields, 
                        how = 'left', right_index = True, left_index=True)

    return result

def process_event_files(infiles, outfile, provider_name):

    # Denote positions of the data in the XML string
    if provider_name == 'Windows Error Reporting':
        attr_dict = {"Data_1": 0,
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
        
    # Define an output dataframe for accumulating data
    result_df = pd.DataFrame()

    for infile in infiles:
        print("beginning to process " + infile)
        t1=time.time()

        # Read in the parquet file and get the generator
        dataframes = read_parquet(infile, provider_name)

        for df in dataframes:

            if provider_name in ('Application Hang', 'Application Error'):

                # Parse the XML 
                df['dict_EventData'] = df['EventDataXML'].apply(lambda x: xml_to_col(x, provider_name))

            elif provider_name == 'Windows Error Reporting':

                # Parse the XML 
                df['dict_EventData'] = df['EventDataXML'].apply(lambda x: parse_winerror_xml(x, attr_dict))
            
            else:
                # Parse boot XML 
                df['dict_EventData'] = df['EventDataXML'].apply(lambda x: parse_boot_xml(x))
            
            # Call the data processing function 
            out_df = expand_dict_values(df)

            # Append to the output df 
            result_df = pd.concat([result_df, out_df], axis=0)
        
        t2=time.time()
        print(("It takes %s seconds to process "+infile) % (t2 - t1))

    # drop unneeeded columns
    dont_save = ['EventDataXML', 'dict_EventData']
    result_df = result_df[[col for col in result_df.columns if col not in dont_save]]

    # Save result to parquet
    result_df.to_parquet(outfile)


if __name__ == '__main__':

    files_to_process = [r"assets\Persist_EventBootResult.parquet"]
    process_event_files(files_to_process, r"assets\Boot_Events_PARSED.parquet", 'Boot')
