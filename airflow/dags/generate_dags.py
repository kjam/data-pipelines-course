""" Simple example of creating subdags and generating work dynamically"""
from airflow import DAG
from airflow.hooks import SqliteHook
from airflow.operators import BashOperator, EmailOperator, SubDagOperator, \
    PythonOperator, BranchPythonOperator
from twitter_airflow import search_twitter, RAW_TWEET_DIR
from subdags.twitter_subdag import subdag
from datetime import datetime, timedelta
import pandas as pd
import re
import random


SEARCH_TERMS = ['#python', '#pydata', '#airflow', 'data wrangling',
                'data pipelines']


default_args = {
    'owner': 'admin',
    'depends_on_past': False,
    'start_date': datetime.now() - timedelta(days=4),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('generate_twitter_dags', default_args=default_args,
          schedule_interval='@daily')


def fill_terms(my_terms=SEARCH_TERMS, **kwargs):
    """ Fill sqlite database with a few search terms. """
    sqlite = SqliteHook('twitter_sqlite')
    conn = sqlite.get_conn()
    df = pd.DataFrame(my_terms, columns=['search_term'])
    try:
        df.to_sql('twitter_terms', conn)
    except ValueError:
        # table already exists
        pass


def generate_search_terms(**kwargs):
    """ Generate subdag to search twitter for terms. """
    sqlite = SqliteHook('twitter_sqlite')
    conn = sqlite.get_conn()
    query = "select * from twitter_terms"
    df = pd.read_sql_query(query, conn)
    return random.choice([
        'search_{}_twitter'.format(re.sub(r'\W+', '', t))
        for t in df.search_term.values])


fill_search_terms = PythonOperator(task_id='fill_terms',
                                   provide_context=True,
                                   python_callable=fill_terms,
                                   dag=dag)


gen_search_terms = BranchPythonOperator(task_id='generate_search_terms',
                                        provide_context=True,
                                        python_callable=generate_search_terms,
                                        dag=dag)


email_links = EmailOperator(task_id='email_best_links',
                            to='katharine@kjamistan.com',
                            subject='Latest popular links',
                            html_content='Check out the latest!!',
                            files=['{}/latest_links.txt'.format(RAW_TWEET_DIR)],
                            dag=dag)


sub = SubDagOperator(subdag=subdag,
                     task_id='insert_and_id_pop',
                     trigger_rule='one_success',
                     dag=dag)


clear_latest = BashOperator(bash_command='rm -rf {}/latest_links.txt'.format(
    RAW_TWEET_DIR), task_id='clear_latest', dag=dag)


gen_search_terms.set_upstream(fill_search_terms)

for term in SEARCH_TERMS:
    term_without_punctuation = re.sub(r'\W+', '', term)
    simple_search = PythonOperator(
        task_id='search_{}_twitter'.format(term_without_punctuation),
        provide_context=True,
        python_callable=search_twitter,
        dag=dag,
        params={'query': term})
    simple_search.set_upstream(gen_search_terms)
    simple_search.set_downstream(sub)

sub.set_downstream(email_links)
email_links.set_downstream(clear_latest)
