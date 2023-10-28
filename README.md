# End-to-End Project — PostgreSQL, Airflow, Docker, Pandas
## Overview 
In this article, we will import a CSV file from a remote repository, make a local PostgreSQL database, and use the load_csv_to_postgres.py script to write the CSV data to the PostgreSQL table.

Next, we'll retrieve the information from the table. Using the create_df_and_modify.py script, we will produce three distinct data frames following a few adjustments and pandas practices.

Ultimately, these three data frames will be obtained, relevant tables will be created in the PostgreSQL database, and load_df_to_postgres.py will be used to put the data frames into these tables.

With the DAG script, each of these scripts will execute as an Airflow DAG job.

Consider this project as a pandas exercise and a different approach to locally storing the data.

![image](https://github.com/quanganh247-qa/PostgreSQL-Airflow-Docker-Pandas/assets/125935864/8278cf76-7d07-44a0-a9e6-95b85d449043)


## Technically
- Apache Airflow
- PostgreSQL
- Pandas
- Python
- Docker

## Get data from CSV

We have to first download PgAdmin to visually see the created tables and run SQL queries. Using pgAdmin is another approach, we can also connect our PostgreSQL instance to PgAdmin . Once prompted on the configuration page, we have to define:

- Host
- Database Name
- User name
- Password
- Port
  
All of these criteria are required in order to connect to the database. If you would want to establish a new database, user, and password, you may view the article below. The port will be 5432 , and we will use localhost. We can also use these parameters to establish a connection to pyAdmin4.

We have to connect to the Postgres server.

```Python
try:
    conn = psycopg2.connect(database = "airflow", 
                                user = "airflow", 
                                host= "postgres",
                                password = "airflow",
                                port = 5432)   
    cur = conn.cursor()
    print('Postgres server connection is successful')
except Exception as e:
    traceback.print_exc()
    print("Couldn't create the Postgres connection")
```
Net satge, we upload data of CSV file into Postgres table:

```Python
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
```
In the end, we have to insert all the data (pandas data frame) into the newly created table row by row

```Python
def write_to_database():
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
```

## Change the Data Frames

The data is retrieved and modified in the second section from the Postgres database.There are several interesting exercises in this section. This section is adaptable and changeable.

We have to first get the main data frame and modify it a bit (Assuming we already created the Postgres connection).

```Python
def create_df_from_dtb(cur):
    cur.execute("""SELECT * FROM churn_data""")
    rows = cur.fetchall()
    """retrieve the column names from the result set of a query executed"""
    col_names = [desc[0] for desc in cur.description]
    df = pd.DataFrame(rows, columns=col_names)

    df.drop('rownumber', axis=1, inplace=True)

    most_occured_country = df['geography'].value_counts().index[0]
    df['geography'].fillna(value=most_occured_country, inplace=True)
    
    avg_balance = df['balance'].mean()
    df['balance'].fillna(value=avg_balance, inplace=True)

    median_creditscore = df['creditscore'].median()
    df['creditscore'].fillna(value=median_creditscore, inplace=True)

    return df
```

## Add the New Data Frames to the PostgreSQL database.

We have to first create 3 tables in the Postgres server.

```Python
def create_new_tables_in_postgres():
    try:
        cur.execute("""CREATE TABLE IF NOT EXISTS churn_modelling_creditscore (geography VARCHAR(50), gender VARCHAR(20), avg_credit_score FLOAT, total_exited INTEGER)""")
        cur.execute("""CREATE TABLE IF NOT EXISTS churn_modelling_exited_age_correlation (geography VARCHAR(50), gender VARCHAR(20), exited INTEGER, avg_age FLOAT, avg_salary FLOAT,number_of_exited_or_not INTEGER)""")
        cur.execute("""CREATE TABLE IF NOT EXISTS churn_modelling_exited_salary_correlation  (exited INTEGER, is_greater INTEGER, correlation INTEGER)""")
        print("3 tables created successfully in Postgres server")
    except Exception as e:
        traceback.print_exc()
        logging.error(f'Tables cannot be created due to: {e}')
```

Following this phase, we must once again confirm that the tables were successfully created. We will finally put each of the three data frames into the Postgres tables.

```Python
def insert_creditscore_table(df_creditscore):
    query = "INSERT INTO churn_modelling_creditscore (geography, gender, avg_credit_score, total_exited) VALUES (%s,%s,%s,%s)"
    row_count = 0
    for _, row in df_creditscore.iterrows():
        values = (row['geography'],row['gender'],row['avg_credit_score'],row['total_exited'])
        cur.execute(query,values)
        row_count += 1
    print(f"{row_count} rows inserted into table churn_modelling_creditscore")

def insert_exited_age_correlation_table(df_exited_age_correlation):
    query = """INSERT INTO churn_modelling_exited_age_correlation (Geography, Gender, exited, avg_age, avg_salary, number_of_exited_or_not) VALUES (%s,%s,%s,%s,%s,%s)"""
    row_count = 0
    for _, row in df_exited_age_correlation.iterrows():
        values = (row['geography'],row['gender'],row['exited'],row['avg_age'],row['avg_salary'],row['number_of_exited_or_not'])
        cur.execute(query,values)
        row_count += 1
    print(f"{row_count} rows inserted into table churn_modelling_exited_age_correlation")

def insert_exited_salary_correlation_table(df_exited_salary_correlation):
    query = """INSERT INTO churn_modelling_exited_salary_correlation (exited, is_greater, correlation) VALUES (%s,%s,%s)"""
    row_count = 0
    for _, row in df_exited_salary_correlation.iterrows():
        values = (int(row['exited']),int(row['is_greater']),int(row['correlation']))
        cur.execute(query,values)
        row_count += 1
    print(f"{row_count} rows inserted into table churn_modelling_exited_salary_correlation")
```

## Airflow DAG

We have to automate the process a bit even though it is not complicated. We are going to do that using the Airflow DAGs.

Run the following command to clone the necessary repo on your local
```
git clone https://github.com/dogukannulu/docker-airflow.git
```
You can use the below docker-compose.yaml file to run the Airflow as a Docker container.
```docker-compose -f docker-compose-LocalExecutor.yml up -d```

Now you have a running Airflow container and you can access the UI at https://localhost:8080. If some errors occur with the libraries and packages, we can go into the Airflow container and install them all manually
![image](https://github.com/quanganh247-qa/PostgreSQL-Airflow-Docker-Pandas/assets/125935864/cb741a87-9f0e-47a7-aef1-f7a718ec2f3a)

After running our Airflow DAG, we can see that the initial table is created inside our PostgreSQL server

![image](https://github.com/quanganh247-qa/PostgreSQL-Airflow-Docker-Pandas/assets/125935864/ac0402e2-fac3-4b1d-9829-42cebd93f11b)

The CSV data is then added to it. Following this, the data will be edited and published into the Postgres server's freshly made tables. By connecting pgAdmin, we can verify each step.

