import pyodbc


def execute_query(q, conn_params, row_expression):
    connection = pyodbc.connect(conn_params)
    cursor = connection.cursor()

    cursor.executemany(q, row_expression)
    connection.commit()

    connection.close()


def multi_row_insert(batch, insert_query, conn_params):
    row_expressions = []

    for _ in range(batch.qsize()):
        row_data = tuple(batch.get())
        row_expressions.append(row_data)

    execute_query(insert_query, conn_params, row_expressions)
