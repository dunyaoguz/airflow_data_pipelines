from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.sparkify_plugin import (StageToRedshiftOperator,
                                               LoadFactOperator,
                                               LoadDimensionOperator,
                                               DataQualityOperator,
                                               CreateTablesOperator)
from helpers import *

default_args = {
    'owner': 'dunya_oguz',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email': ['dunyaoguz13@gmail.com'],
    'email_on_retry': False
}

dag = DAG('sparkify_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *',
          catchup=False
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

create_tables = CreateTablesOperator(
    task_id= 'Create_tables',
    dag=dag,
    redshift_conn_id='redshift',
    tables=['staging_events', 'staging_songs', 'artists', 'songplays', 'songs', 'time', 'users'],
    create_statements=[staging_events_table_create, staging_songs_table_create, artists_table_create,
                       songplays_table_create, songs_table_create, time_table_create, users_table_create]
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials='aws_credentials',
    table='staging_events',
    s3_bucket='s3://udacity-dend/log_data',
    copy_json_option='s3://udacity-dend/log_json_path.json'
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials='aws_credentials',
    table='staging_songs',
    s3_bucket='s3://udacity-dend/song_data',
    copy_json_option='auto'
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    table='songplays',
    redshift_conn_id='redshift',
    insert_statement=songplays_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    table='users',
    redshift_conn_id='redshift',
    insert_statement=users_table_insert,
    truncate_insert=True
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    table='songs',
    redshift_conn_id='redshift',
    insert_statement=songs_table_insert,
    truncate_insert=True
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    table='artists',
    redshift_conn_id='redshift',
    insert_statement=artists_table_insert,
    truncate_insert=True
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    table='time',
    redshift_conn_id='redshift',
    insert_statement=time_table_insert,
    truncate_insert=True
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id = 'redshift',
    test_query="SELECT COUNT(*) FROM songplays WHERE songplay_id IS NULL",
    expected_result=0
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

# Configure upsteam/downstream tasks
start_operator >> create_tables
create_tables >> stage_events_to_redshift
create_tables >> stage_songs_to_redshift
stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table
load_song_dimension_table >> run_quality_checks
load_user_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks
run_quality_checks >> end_operator
