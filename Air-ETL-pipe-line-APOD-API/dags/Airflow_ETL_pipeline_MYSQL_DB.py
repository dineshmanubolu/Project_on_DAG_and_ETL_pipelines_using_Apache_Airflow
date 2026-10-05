"""
ETL Pipe line NASA's APOD API -NASA's Astronomy Picture of the Day 

"""

from airflow.sdk import dag,task
from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from datetime import datetime

import json

#Defining DAG logic under @dag decorator
@dag(
        dag_id="Airflow_ETL_Pipeline_APOD_API_MySql_DB",
        start_date=datetime(2025,1,1),
        schedule="@daily",
        catchup=False
)
def main_function():
     #step 1:create table if doesn't exist
     @task
     def create_table():
          ## initialize the Postgreshook
          MySqlHook_hook=MySqlHook(mysql_conn_id="MySql_connection")
          ## Sql query to create the table
          create_table_query=""" 
          CREATE TABLE IF NOT EXISTS apod_data (
            id INT AUTO_INCREMENT PRIMARY KEY,
            title VARCHAR(255),
            explanation TEXT,
            url TEXT,
            date DATE,
            media_type VARCHAR(50)
          );

          """
          ## Execute table creating query
          MySqlHook_hook.run(create_table_query)

        ## Step2 : Extract the NASA API Data(APOD)-Astronomy Picture of the day[Extract Pipeline]
        
     
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
          return apod_data;


     ## step 4:Load the data into MySql SQL

     @task
     def load_data_to_MySql(apod_data):
      ## Initialize the PostgresHook
       MySql_hook=MySqlHook(mysql_conn_id='MySql_connection')

      ## Define the SQL Insert Query

       insert_query = """
          INSERT INTO apod_data (title, explanation, url, date, media_type) VALUES (%s, %s,%s,%s,%s);"""
      ## Execute the SQL Query

       MySql_hook.run(insert_query,parameters=(
          apod_data['title'],
          apod_data['explanation'],
          apod_data['url'],
          apod_data['date'],
          apod_data['media_type']

       ))

   ## step5 : verify the details


   ## step6: define task dependencies
     table_creation=create_table() 

      ## extrate phase using python right bitshift operator
     table_creation >> extract_apod
       
      ## transform phase
     result_dict=transform_apod_data(extract_apod.output)

       ## load phase
     load_data_to_MySql(result_dict)


main_function()