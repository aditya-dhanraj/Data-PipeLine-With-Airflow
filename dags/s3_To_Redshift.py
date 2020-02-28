from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

s3_bucket="s3://udacity-dend/"

default_args = {
    'owner': 'Aditya Dhanraj',
    'depends_on_past': False,
    'catchup': False,
    'start_date': datetime(2020, 2, 19),
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),    
}

dag = DAG(
    's3_To_Redshift',
    default_args=default_args,
    catchup=False,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='@hourly'
)

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    creation_query=SqlQueries.staging_events_table_create,
    table_name="staging_events",
    region="us-west-2",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_path=s3_bucket+"log_data/",
    json_data_format=s3_bucket+"log_json_path.json"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    creation_query=SqlQueries.staging_songs_table_create,
    table_name="staging_songs",
    region="us-west-2",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_path=s3_bucket+"song_data/",
    json_data_format="auto"
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    table_name="songplays",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    creation_query=SqlQueries.songplay_table_create,
    data_insertion_query=SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    table_name="users",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    creation_query=SqlQueries.user_table_create,
    data_insertion_query=SqlQueries.user_table_insert
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    table_name="songs",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    creation_query=SqlQueries.song_table_create,
    data_insertion_query=SqlQueries.song_table_insert
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    table_name="artists",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    creation_query=SqlQueries.artist_table_create,
    data_insertion_query=SqlQueries.artist_table_insert
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    table_name="time",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    creation_query=SqlQueries.time_table_create,
    data_insertion_query=SqlQueries.time_table_insert
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    tables=["staging_events", "staging_songs", "songplays", "users", "songs", "artists", "time"]
)

end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

#***************************Code End****************************************
# Task Dependencies
start_operator >> [stage_events_to_redshift, 
                   stage_songs_to_redshift] >> load_songplays_table

load_songplays_table >> [load_song_dimension_table,
                        load_user_dimension_table,
                        load_artist_dimension_table,
                        load_time_dimension_table] >> run_quality_checks

run_quality_checks >> end_operator 