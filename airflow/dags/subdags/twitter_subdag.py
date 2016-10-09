""" Simple subdag example """
from airflow import DAG
from airflow.operators import PythonOperator
from twitter_airflow import csv_to_sqlite, identify_popular_links
from datetime import datetime, timedelta


default_args = {
    'owner': 'admin',
    'depends_on_past': False,
    'start_date': datetime(2016, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

subdag = DAG('generate_twitter_dags.insert_and_id_pop',
             default_args=default_args)

move_tweets_to_sqlite = PythonOperator(task_id='csv_to_sqlite',
                                       provide_context=True,
                                       python_callable=csv_to_sqlite,
                                       dag=subdag)

id_popular = PythonOperator(task_id='identify_popular_links',
                            provide_context=True,
                            python_callable=identify_popular_links,
                            dag=subdag,
                            params={'write_mode': 'a'})

id_popular.set_upstream(move_tweets_to_sqlite)
