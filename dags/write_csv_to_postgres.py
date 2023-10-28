
import psycopg2
import os
import traceback
import logging
import pandas as pd
import urllib.request
dest_folder = os.environ.get('D:\DE\Project\csv_extract_airflow_docker\csv_files')

try:
    conn = psycopg2.connect(database = "postgres", 
                                user = "postgres", 
                                host= 'localhost',
                                password = "123456",
                                port = 5432)   
    cur = conn.cursor()
    print('Postgres server connection is successful')
except Exception as e:
    traceback.print_exc()
    print("Couldn't create the Postgres connection")


def create_table():
    """
    Create the Postgres table with a desired schema
    """
    try:
        cur.execute("""CREATE TABLE IF NOT EXISTS churn_data (
                    RowNumber INTEGER PRIMARY KEY, CustomerId INTEGER, 
                    Surname VARCHAR(50), 
                    CreditScore INTEGER, Geography VARCHAR(50), 
                    Gender VARCHAR(20), 
                    Age INTEGER, Tenure INTEGER,
                    Balance FLOAT, NumOfProducts INTEGER, 
                    HasCrCard INTEGER, IsActiveMember INTEGER, 
                    EstimatedSalary FLOAT, Exited INTEGER)""")
        
        print('New table created successfully to postgres server')
    except:
        print('Check if the table exists')


def write_to_database():
    """
    Create the dataframe and write to Postgres table if it doesn't already exist
    """
    df = pd.read_csv('D:\DE\Project\csv_extract_airflow_docker\csv_files\churn_modelling.csv')
    inserted_row_count = 0

    for _, row in df.iterrows():
        count_query = f"""SELECT COUNT(*) FROM churn_data WHERE RowNumber = {row['RowNumber']}"""
        cur.execute(count_query)
        result = cur.fetchone()
        
        if result[0] == 0:
            inserted_row_count += 1
            cur.execute("""INSERT INTO churn_data (RowNumber, CustomerId, Surname, CreditScore, Geography, Gender, Age, 
            Tenure, Balance, NumOfProducts, HasCrCard, IsActiveMember, EstimatedSalary, Exited) VALUES (%s, %s, %s,%s, %s, %s,%s, %s, %s,%s, %s, %s,%s, %s)""", 
            (int(row[0]), int(row[1]), str(row[2]), int(row[3]), str(row[4]), str(row[5]), int(row[6]), int(row[7]), float(row[8]), int(row[9]), int(row[10]), int(row[11]), float(row[12]), int(row[13])))

    print(f' {inserted_row_count} rows from csv file inserted into table successfully')



if __name__ == '__main__':
    create_table()
    write_to_database()
    conn.commit()
    cur.close()
    conn.close()
