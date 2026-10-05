"""
ETL Pipe line NASA's APOD API -NASA's Astronomy Picture of the Day 

"""

from airflow.sdk import dag,task
# from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime

import json

#Defining DAG logic under @dag decorator
@dag(
        dag_id="Airflow_ETL_Pipeline_APOD_API_postgres_DB",
        start_date=datetime(2025,1,1),
        schedule="@daily",
        catchup=False
)
def main_function():
     #step 1:create table if doesn't exist
     @task
     def create_table():
          ## initialize the Postgreshook
          Postgres_hook=PostgresHook(postgres_conn_id="my_postgres_connection")
          ## Sql query to create the table
          create_table_query=""" 
          CREATE TABLE IF NOT EXISTS apod_data(
           id SERIAL PRIMARY KEY,
           title VARCHAR(255),
           explanation TEXT,
           url TEXT,
           date DATE,
           media_type VARCHAR(50)

            );

          """
          ## Execute table creating query
          Postgres_hook.run(create_table_query)

        ## Step2 : Extract the NASA API Data(APOD)-Astronomy Picture of the day[Extract Pipeline]
        ## https://api.nasa.gov/planetary/apod?api_key=1c6dQfFHU6C2dPBdkme4F83qUIdKinuylLdsUczE
     
     # extract_apod=SimpleHttpOperator(
     extract_apod=HttpOperator(
               task_id='extract_apod',
               http_conn_id='nasa_api',  #connection ID Defined in Airflow for NASA API
               endpoint='planetary/apod', #NASA API end point for APOD
               method='GET',
               data={"api_key":"{{conn.nasa_api.extra_dejson.api_key}}"}, #used API key
               response_filter=lambda response:response.json(), ## Convert response to json
           )

     ## Step3: transform the data
     @task
     def transform_apod_data(response):
          apod_data={
               'title':response.get('title',''),
               'explanation':response.get('explanation',''),
               'url':response.get('url',''),
               'date':response.get('date',''),
               "media_type":response.get('media_type','')
          }
          return apod_data


     ## step 4:Load the data into Postgres SQL

     @task
     def load_data_to_postgres(apod_data):
      ## Initialize the PostgresHook
       postgres_hook=PostgresHook(postgres_conn_id='my_postgres_connection')

      ## Define the SQL Insert Query

       insert_query = """
          INSERT INTO apod_data (title, explanation, url, date, media_type)
           VALUES (%s, %s,%s,%s,%s);
            """
      ## Execute the SQL Query

       postgres_hook.run(insert_query,parameters=(
          apod_data['title'],
          apod_data['explanation'],
          apod_data['url'],
          apod_data['date'],
          apod_data['media_type']

       ))

   ## step5 : verify the details


   ## step6: define task dependencies
     table_creation=create_table() 

      ## extrate phase
     table_creation >> extract_apod
       
      ## transform phase
     result_dict=transform_apod_data(extract_apod.output)

       ## load phase
     load_data_to_postgres(result_dict)


main_function()