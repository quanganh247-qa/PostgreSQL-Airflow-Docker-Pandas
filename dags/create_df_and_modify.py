"""
Creates a base dataframe out of churn_modelling table and creates 3 separate dataframes out of it.
"""
import os
import logging
import psycopg2
import traceback
import numpy as np
import pandas as pd

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s:%(funcName)s:%(levelname)s:%(message)s')

try:
    conn = psycopg2.connect(database = "postgres", 
                            user = "postgres", 
                            host= 'localhost',
                            password = "123456",
                            port = 5432)   
    cur = conn.cursor()
    logging.info('Postgres server connection is successful')
except Exception as e:
    traceback.print_exc()
    logging.error("Couldn't create the Postgres connection")


def create_df_from_dtb(cur):
    """
    Base dataframe of churn_data table
    """
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


def creditscore_df(df):
    df_creditscore = df[['geography', 'gender', 'exited', 'creditscore']].groupby(['geography','gender']).agg({'creditscore':'mean', 'exited':'sum'})
    df_creditscore.rename(columns={'exited':'total_exited', 'creditscore':'avg_credit_score'}, inplace=True)
    df_creditscore.reset_index(inplace=True)
    df_creditscore.sort_values('avg_creditscore', inplace=True)
    return df_creditscore


def exited_age_correlation(df):
    exited_age_correlation = df.groupby(['geography', 'gender', 'exited']).agg({
    'age': 'mean',
    'estimatedsalary': 'mean',
    'exited': 'count'
    }).rename(columns={
        'age': 'avg_age',
        'estimatedsalary': 'avg_salary',
        'exited': 'exited_or_not'
    }).reset_index().sort_values('exited_or_not')

    return exited_age_correlation


def exited_salary_correlation(df):
    df_salary = df[['geography','gender','exited','estimatedsalary']].groupby([
        'geography','gender']).agg({'estimatedsalary':'mean'
                            }).sort_values('estimatedsalary')
    
    df_salary.reset_index(inplace=True)

    min_salary = round(df_salary['estimatedsalary'].min(),0)

    df['is_greater'] = df['estimatedsalary'].apply(lambda x: 1 if x>min_salary else 0)

    df_exited_salary_correlation = pd.DataFrame({
    'exited': df['exited'],
    'is_greater': df['estimatedsalary'] > df['estimatedsalary'].min(),
    'correlation': np.where(df['exited'] == (df['estimatedsalary'] > df['estimatedsalary'].min()), 1, 0)
    })

    return df_exited_salary_correlation


