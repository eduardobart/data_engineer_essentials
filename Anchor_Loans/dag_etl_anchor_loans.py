import airflow
from airflow import DAG
import numpy as np
import pandas as pd
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
import cx_Oracle
from sqlalchemy import types, create_engine
import os


default_args = {
 'owner':'Bartolomeu',
 'start_date': airflow.utils.dates.days_ago(1),
 'depends_on_past': False,
 'retries':0,
 }

#DAG description and schedule
dag = DAG(
    'DAG_ETL_ANCHOR_LOANS',
    default_args=default_args,
    description='DAG that reads file with loans data, transforms some fields and import the into Oracle Database',
    schedule_interval='0 2 1 * *', # executes every day 1, at 02:00 AM
 )
#Arguments that we will use in the DAG.
kwargs = {'filepath': '/home/bartolomeu/Downloads/Datasets/'
         ,'fields': ['ENTERPRISE_FLAG', 'RECORD_NUMBER', 'US_POSTAL_STATE_CODE', 'METROPOLITAN_STA_AREA_MSA_CODE', 'COUNTY_2010_CENSUS', 'CENSUS_TRACT_2010_CENSUS', '2010_CENSUS_TRACT_PCT_MINORITY'
                    ,'2010_CENSUS_TRACT_MEDIAN_INCOM', 'LOCAL_AREA_MEDIAN_INCOME', 'TRACT_INCOME_RATIO', 'BORROWERS_ANNUAL_INCOME', 'AREA_MEDIAN_FAMILY_INCOME_2018', 'BORROWER_INCOME_RATIO'
                    ,'ACQ_UNPAID_PRINCIP_BALANCE_UPB', 'PURPOSE_OF_LOAN', 'FEDERAL_GUARANTEE', 'NUMBER_OF_BORROWERS', 'FIRST_TIME_HOME_BUYER', 'BORROWER_RACE_1', 'BORROWER_RACE_2', 'BORROWER_RACE_3'
                    ,'BORROWER_RACE_4', 'BORROWER_RACE_5', 'BORROWER_ETHNICITY', 'CO_BORROWER_RACE_1', 'CO_BORROWER_RACE_2', 'CO_BORROWER_RACE_3', 'CO_BORROWER_RACE_4', 'CO_BORROWER_RACE_5'
                    ,'CO_BORROWER_ETHNICITY', 'BORROWER_GENDER', 'CO_BORROWER_GENDER', 'AGE_OF_BORROWER', 'AGE_OF_CO_BORROWER', 'OCCUPANCY_CODE', 'RATE_SPREAD', 'HOEPA_STATUS', 'PROPERTY_TYPE'
                    , 'LIEN_STATUS']
         , 'connection_string':'oracle+cx_oracle://DB_AL:DB_AL@localhost:1521/?service_name=xe'
         , 'table_name':'LOANS'
         , 'schema_name':'DB_AL'}

def etl_read_file_import_oracle(**kwargs):
  #For every file in path
  for file in os.listdir(kwargs.get('filepath')):
    #Read the file       
    df = pd.read_fwf(kwargs.get('filepath') + file, delim_whitespace=True, names=kwargs.get('fields'))
    #Transformation Block
    df['PURPOSE_OF_LOAN'] = np.where((df['PURPOSE_OF_LOAN'].astype(str)=='1'), 'Purchase', df['PURPOSE_OF_LOAN'])
    df['PURPOSE_OF_LOAN'] = np.where((df['PURPOSE_OF_LOAN'].astype(str)=='2'), 'Refinancing', df['PURPOSE_OF_LOAN'])
    df['PURPOSE_OF_LOAN'] = np.where((df['PURPOSE_OF_LOAN'].astype(str)=='4'), 'Home Improvement/Rehabilitation', df['PURPOSE_OF_LOAN'])
    df['PURPOSE_OF_LOAN'] = np.where((df['PURPOSE_OF_LOAN'].astype(str)=='9'), 'Not applicable/not available', df['PURPOSE_OF_LOAN'])

    df['AGE_OF_BORROWER'] = np.where((df['AGE_OF_BORROWER'].astype(str)=='1'), 'under 25 years old', df['AGE_OF_BORROWER'])
    df['AGE_OF_BORROWER'] = np.where((df['AGE_OF_BORROWER'].astype(str)=='2'), '25 to 34 years old', df['AGE_OF_BORROWER'])
    df['AGE_OF_BORROWER'] = np.where((df['AGE_OF_BORROWER'].astype(str)=='3'), '35 to 44 years old', df['AGE_OF_BORROWER'])
    df['AGE_OF_BORROWER'] = np.where((df['AGE_OF_BORROWER'].astype(str)=='4'), '45 to 54 years old', df['AGE_OF_BORROWER'])
    df['AGE_OF_BORROWER'] = np.where((df['AGE_OF_BORROWER'].astype(str)=='5'), '55 to 64 years old', df['AGE_OF_BORROWER'])
    df['AGE_OF_BORROWER'] = np.where((df['AGE_OF_BORROWER'].astype(str)=='6'), '65 to 74 years old', df['AGE_OF_BORROWER'])
    df['AGE_OF_BORROWER'] = np.where((df['AGE_OF_BORROWER'].astype(str)=='7'), 'over 74 years old', df['AGE_OF_BORROWER'])
    df['AGE_OF_BORROWER'] = np.where((df['AGE_OF_BORROWER'].astype(str)=='9'), 'data not provided', df['AGE_OF_BORROWER'])

    df['OCCUPANCY_CODE'] = np.where((df['OCCUPANCY_CODE'].astype(str)=='1'), 'Owner-Occupied property', df['OCCUPANCY_CODE'])
    df['OCCUPANCY_CODE'] = np.where((df['OCCUPANCY_CODE'].astype(str)=='2'), 'Investment property', df['OCCUPANCY_CODE'])
    df['OCCUPANCY_CODE'] = np.where((df['OCCUPANCY_CODE'].astype(str)=='9'), 'Not Available', df['OCCUPANCY_CODE'])

    #Declare variable to connection string
    conn = create_engine(kwargs.get('connection_string'))
    #Divides the dataframe in chunks to import in the Oracle.
    for chunk in np.array_split(df, 1000):
      chunk.to_sql(kwargs.get('table_name'), conn, if_exists='append', index=False, chunksize=10000, schema=kwargs.get('schema_name'))

    
#Dummy Tasks
start = DummyOperator(task_id='start', dag=dag)
end = DummyOperator(task_id='end', dag=dag) 

#Main Task
tsk_etl_file_anchor_loans = PythonOperator(
        task_id='tsk_etl_file_anchor_loans',
        python_callable=etl_read_file_import_oracle,
        op_kwargs=kwargs,
        dag=dag,
    )
#Tasks order
start >> tsk_etl_file_anchor_loans >> end