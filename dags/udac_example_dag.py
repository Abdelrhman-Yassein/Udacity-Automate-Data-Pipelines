# Import Libraries
from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                               LoadDimensionOperator, DataQualityOperator, PythonOperator,
                               PostgresOperator)
from helpers import SqlQueries
import create_tables

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'catchup': False,
}

# Create New Dag With Name udac_example_dag
dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@once'
          #           schedule_interval='0 * * * *'
          )

# Create Start Task With No Thing Just Start
start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

# create_redshift_tables To Create Table In DataBase
# Sql Query From create_tables.py
create_redshift_tables = PostgresOperator(
    task_id="create_redshift_table",
    dag=dag,
    postgres_conn_id="redshift",  # connection we created in airflow ui admin connection
    # SQL Query To Create All Tables From create_tables.py
    sql=create_tables.CREATE_TABLES_SQL
)


# stage_events_to_redshift Task To Extract Data From Files To Staging Table
stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table="staging_events",  # Table Name
    region="us-west-2",  # Region On AWS
    s3_key="log_data",  # S3 Key - s3://udacity-dend/log_data
    s3_bucket="udacity-dend",  # S3 Buket - s3://udacity-dend/log_data
    file_formate='json',  # Files Formate
    provide_context=True,  # kwargs.get('execution_date') To Get execution_date
    redshift_conn_id="redshift",  # connection we created in airflow ui admin connection
    # connection we created in airflow ui admin connection
    aws_credentials_id="aws_credentials"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table="staging_songs",  # Table Name
    region="us-west-2",  # Region On AWS
    s3_key="log_data",  # S3 Key - s3://udacity-dend/song_data
    s3_bucket="udacity-dend",  # S3 Buket - s3://udacity-dend/song_data
    file_formate='json',  # Files Formate
    provide_context=True,  # kwargs.get('execution_date') To Get execution_date
    redshift_conn_id="redshift",  # connection we created in airflow ui admin connection
    # connection we created in airflow ui admin connection
    aws_credentials_id="aws_credentials",
)

# load_songplays_table Task To Extract fact_table Data From staging_events Table
load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    # SQL Query From plugins/helper/sl_ueries.py
    sql_query=SqlQueries.songplay_table_insert,
    redshift_conn_id="redshift",  # connection we created in airflow ui admin connection
    # connection we created in airflow ui admin connection
    aws_credentials_id="aws_credentials"
)

# load_user_dimension_table Task To Extract users Data From staging_events Table
load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    table="users",  # Table Name
    truncat=True,  # To Execute Truncate Query
    # SQL Query From plugins/helper/sl_ueries.py
    sql_query=SqlQueries.user_table_insert,
    redshift_conn_id="redshift",  # connection we created in airflow ui admin connection
    # connection we created in airflow ui admin connection
    aws_credentials_id="aws_credentials"
)

# load_song_dimension_table Task To Extract songs Data From staging_songs Table
load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    table="songs",  # Table Name
    truncat=True,  # To Execute Truncate Query
    # SQL Query From plugins/helper/sl_ueries.py
    sql_query=SqlQueries.song_table_insert,
    redshift_conn_id="redshift",  # connection we created in airflow ui admin connection
    # connection we created in airflow ui admin connection
    aws_credentials_id="aws_credentials"
)

# load_artist_dimension_table Task To Extract songs Data From staging_songs Table
load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    table="artists",  # Table Name
    truncat=True,  # To Execute Truncate Query
    # SQL Query From plugins/helper/sl_ueries.py
    sql_query=SqlQueries.song_table_insert,
    redshift_conn_id="redshift",  # connection we created in airflow ui admin connection
    # connection we created in airflow ui admin connection
    aws_credentials_id="aws_credentials"
)

# load_time_dimension_table Task To Extract songs Data From songplays Table
load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    table="time",  # Table Name
    truncat=True,  # To Execute Truncate Query
    # SQL Query From plugins/helper/sl_ueries.py
    sql_query=SqlQueries.time_table_insert,
    redshift_conn_id="redshift",  # connection we created in airflow ui admin connection
    # connection we created in airflow ui admin connection
    aws_credentials_id="aws_credentials"
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    provide_context=True,
    # connection we created in airflow ui admin connection
    aws_credentials_id="aws_credentials",
    redshift_conn_id='redshift',  # connection we created in airflow ui admin connection
    # Tables We Create Check For It
    tables=["public.songplays", "public.users",
            "public.songs", "public.artists", "public.time"]
)
# Create End Task With No Thing Just End
end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


start_operator >> create_redshift_tables >> [
    stage_songs_to_redshift, stage_events_to_redshift]

[stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table

load_songplays_table >> [load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table,
                         load_time_dimension_table] >> run_quality_checks

run_quality_checks >> end_operator
