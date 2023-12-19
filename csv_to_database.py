import pandas as pd
import pymysql


def csv_to_sql(random_name, csv_file):
    try:
        df = pd.read_csv(csv_file).fillna('')
    except:
        try:
            df = pd.read_csv(csv_file, encoding='latin-1')
        except:
            print('Failed Converting')
            return False
    headers = []
    for k in df.items():
        key = k[0]
        headers.append(key)

    """
        Create Table / Model Dynamic Using SQL Query
    """
    try:
        create_model_sql(random_name, df, headers)
    except:
        print('Failed Converting')
        return False

    print(
        f'\nSQL QUERY =  SELECT * FROM {database}.{random_name};'
        f'\nTABLE NAME = {random_name}'
    )


def connection_db():
    global database
    host = 'localhost'
    port = 3306
    user = 'root'
    passwd = 'password'
    database = 'crawlmagic'
    con = pymysql.connect(host=host, port=port, user=user, passwd=passwd, database=database)
    return con


def create_model_sql(random_name, df, headers):
    try:
        conn = connection_db()
        cursor = conn.cursor()
    except Exception as e:
        print(f'Connection Error = {e}')
        return False

    # Create columns string
    columns = ", ".join([name + " LONGTEXT" for name in headers])

    # Create table
    create_query = f"CREATE TABLE if not exists {random_name} ({columns})"
    cursor.execute(create_query)

    # Insert Data
    try:
        insert_query = f"INSERT IGNORE INTO {random_name} ({', '.join([i for i in headers])}) VALUES ({', '.join(['%s' for i in headers])})"
        temp_data = []
        for i, row in df.iterrows():
            kwargs = []
            for header in headers:
                kwargs.append(row[header])
            data_test = tuple(kwargs)
            temp_data.append(data_test)
        cursor.executemany(insert_query, temp_data)
    except Exception as e:
        print(f'Insert Data Error = {e}')

    conn.commit()
    cursor.close()
    conn.close()


if __name__ == '__main__':
    random_name = input("Enter Table Name :: ")
    csv_file = input("Enter CSV Name :: ")
    csv_to_sql(random_name, csv_file)
