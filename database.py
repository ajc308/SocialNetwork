import config
import pymssql


server = config.db_server
user = config.db_user
password = config.db_password
database = config.db_name
application_configuration_table_name = config.application_configuration_table_name


def get_connection(as_dict=False):
    return pymssql.connect(server=server,
                           user=user,
                           password=password,
                           database=database,
                           as_dict=as_dict
    )


def execute_query(query):
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute(query)
    conn.commit()

    cursor.close()
    conn.close()

def get_application_configuration():
    query = "SELECT * FROM {} WHERE One = 1".format(application_configuration_table_name)

    conn = get_connection(as_dict=True)
    cursor = conn.cursor()

    cursor.execute(query)
    results = cursor.fetchone()

    cursor.close()
    conn.close()

    return results


def get_table_columns(table_name, as_dict=False):
    query = "SELECT c.name FROM sys.objects o INNER JOIN sys.columns c ON o.object_id = c.object_id WHERE o.name='{table_name}'".format(
        table_name=table_name
    )
    conn = get_connection(as_dict)
    cursor = conn.cursor()

    cursor.execute(query)
    table_columns = [row[0] for row in cursor]

    cursor.close()
    conn.close()

    return table_columns


def truncate_table(table_name):
    execute_query("TRUNCATE TABLE [{table_name}]".format(table_name=table_name))


def select_from_table(table_name, column_names=None, where_data=None, where_string=None, as_dict=False):
    if where_data and where_string:
        raise ValueError('You can only pass where_data or where_string, not both.')

    column_names = ', '.join(map(str, column_names)) if column_names else '*'
    conn = get_connection(as_dict)
    cursor = conn.cursor()

    select_query = "SELECT {column_names} FROM [{table_name}]".format(column_names=column_names, table_name=table_name)

    if where_data:
        where_columns = generate_equivalences_from_dict(data=where_data)
        select_query += ' WHERE ' + ', '.join(map(str, where_columns))

    if where_string:
        select_query += ' WHERE ' + where_string

    cursor.execute(select_query)
    results = cursor.fetchall()

    cursor.close()
    conn.close()

    return results


def generate_insert_query(table_name, columns, data):
    insert_columns = ', '.join(map(str, columns)) + ', UpdatedOn'
    insert_query = 'INSERT INTO {table_name} ({columns}) VALUES '.format(table_name=table_name, columns=insert_columns)

    query_values = []
    for column in columns:
        if type(data[column]) == int:
            query_values.append(str(data[column]))
        elif type(data[column]) == bool:
            query_values.append(str(int(data[column])))
        elif type(data[column]) not in (int, bool, dict, list):
            query_values.append("'" + data[column].replace("'", "''") + "'")

    insert_query += ' (' + ', '.join(map(str, query_values)) + ', GETDATE())'

    return insert_query


def generate_update_query(table_name, columns, data, where_data=None):
    update_query = 'UPDATE [{table_name}] SET '.format(table_name=table_name)

    update_columns = generate_equivalences_from_dict(data=data, columns=columns)
    update_query += ', '.join(map(str, update_columns)) + ', UpdatedOn = GETDATE()'

    if where_data:
        where_columns = generate_equivalences_from_dict(data=where_data)
        update_query += ' WHERE ' + ', '.join(map(str, where_columns))
    return update_query


def generate_equivalences_from_dict(data, columns=None):
    if data:
        columns = data.keys() if not columns else columns

        equivalences = ['[' + column + "] = " + str(data[column]).replace("'", "''") for column in columns if type(data[column]) == int]
        equivalences.extend(['[' + column + "] = " + str(int(data[column])) for column in columns if type(data[column]) == bool])
        equivalences.extend(['[' + column + "] = '" + data[column].replace("'", "''") + "'" for column in columns if type(data[column]) not in (int, bool, dict, list)])

        return equivalences
    else:
        return None
