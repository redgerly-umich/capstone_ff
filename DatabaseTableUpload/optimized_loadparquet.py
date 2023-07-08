# Import python standard libraries
import json 
from concurrent import futures
import queue
import time

# Load non-standard libraries
import pyarrow.parquet as pq

# Load custom functions
import sqlactions

# Define constants
MULTI_ROW_INSERT_LIMIT = 1000
WORKERS = 12

# Define a time it decorator to test inserts
def timing_decorator(func):
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        print(f"Function {func.__name__} took {end_time - start_time} seconds to run.")
        return result
    return wrapper

def read_parquet(parquet_file):
    ''' 
    Reads a parquet file and returns
    a generator of rows
    '''

    # Read in the parquet file
    parquet_data = pq.ParquetFile(parquet_file)

    # Read parquet data in chunks 
    for chunk in parquet_data.iter_batches():
        
        parquet_df = chunk.to_pandas()
        
        for row in parquet_df.itertuples(index=False):
            yield row

def get_insert_statement(filename, table_name):
    ''' 
    Generates the INSERT SQL used to load the 
    database table based off of the columns 
    in the filename.
    '''

    # Read in the parquet file
    parquet_data = pq.ParquetFile(filename)

    # Get the field names from the parquet file schema
    pq_field_names = parquet_data.schema_arrow.names

    # Igonore index, if present
    if '__index_level_0__' in pq_field_names:
        pq_field_names = [val for val in pq_field_names if val != '__index_level_0__']

    # Get the number of parameters to use in the insert query
    col_tup_str_curr = '(' + ','.join(pq_field_names) +')'
    num_qs_rep = len(col_tup_str_curr.split(','))
    values_ques_str = ','.join(tuple(['?']*num_qs_rep))

    # Define the SQL insert statement
    insert_query = "INSERT INTO " + table_name + " " + col_tup_str_curr + " VALUES (" + values_ques_str + ")"

    # Close parquet file 
    parquet_data.close()

    return insert_query


def process_row(row, batch, insert_query, conn_params):
    ''' 
    Executes a batch load of records to the 
    destination table.
    '''
    batch.put(row)

    if batch.full():
        sqlactions.multi_row_insert(batch, insert_query, conn_params)

    return batch

@timing_decorator
def load_parquet(parquet_file, table_def, conn_params):
    '''
    Loads the parquet data to SQL destination using 
    concurrency. 
    '''

    # Establish a queue object to accumulate data
    batch = queue.Queue(MULTI_ROW_INSERT_LIMIT)

    insert_query = get_insert_statement(parquet_file, table_def['name'])

    # Split load jobs concurrently with threading 
    with futures.ThreadPoolExecutor(max_workers=WORKERS) as executor:
        
        todo = []
        
        # Begin adding jobs to the queue
        for row in read_parquet(parquet_file):
            future = executor.submit(
                process_row, row, batch, insert_query, conn_params
            )
            todo.append(future)

        # Execute jobs across threads
        for i, future in enumerate(futures.as_completed(todo)):

            try:
                result = future.result()
            
            except Exception as e:
                print(f'Failed to load on row {i+1}', '\n\n')
                print('The following error occrred: ', str(e))

    # Handle left overs
    if not result.empty():

        try:
            sqlactions.multi_row_insert(result, insert_query, conn_params)

        except Exception as e:
                print('Failed handling leftovers.')
                print('The following error occrred: ', str(e))


def initiate_load(table_name, pq_file_name):
    ''' 
    Loads database credentials and initiates the
    load to SQL using input parameters.
    '''

    # Get database access info
    with open('assets/fun.json') as f:
        cred = json.load(f)

    # Specify table name
    table_def = {"name": table_name}

    # Create conn string
    conn_params =  f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={cred['server']};DATABASE={cred['db']};UID={cred['uid']};PWD={cred['pwd']}"

    # Execute load to SQL
    load_parquet(pq_file_name, table_def, conn_params)

if __name__ == "__main__":
    
    initiate_load("dbo.[Persist.Time_Zone_DATA_TEST]", "assets/TimeZoneData_TEST.parquet")