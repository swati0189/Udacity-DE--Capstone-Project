import logging
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import Variable
from operators.stage_redshift import StageToRedshiftOperator
from operators.load_fact import LoadFactOperator
from helpers.sql_queries import SqlQueries
from operators.load_dimension import LoadDimensionOperator
from operators.data_quality import DataQualityOperator
#from airflow.operators import (DataQualityOperator, LoadDimensionOperator, LoadFactOperator, StageToRedshiftOperator)
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
#from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

AWS_KEY = AwsHook('aws_credentials').get_credentials().access_key
AWS_SECRET = AwsHook('aws_credentials').get_credentials().secret_key

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False
}

def getS3Bucket():
    return Variable.get("s3_bucket")

def getRedshiftConnId():
    return 'redshift'

dag = DAG('udac_etl_dag1',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@daily',
          catchup=False
          )

dq_checks = [
    {'check_sql': "SELECT COUNT(*) FROM dim_listing WHERE listing_id is null OR date is null OR price = 0 OR available is null OR available = '' ",
     'expected_result': 0,
     'descr': "null values in dim_listing.listig_id column, dim_listing.date column"},
    
    {'check_sql': "SELECT COUNT(*) FROM dim_host WHERE host_id is null",
     'expected_result': 0,
     'descr': "null values in dim_host.host_id column"},
    
    {'check_sql': "SELECT COUNT(*) FROM dim_review WHERE listing_id is null or review_id is null",
     'expected_result': 0,
     'descr': "null values in dim_review.listing_id column, dim_review.review_id column "},
    
    {'check_sql': "SELECT COUNT(*) FROM fact_neighbourhood where number_of_available_listing > total_number_of_listings OR number_of_unavailable_listing > total_number_of_listings OR min_price > avg_price OR max_price < avg_price",
     'expected_result': 0,
     'descr': "null values"},
    
    {'check_sql': "SELECT COUNT(*) FROM fact_listing where listing_id is null Or is_premium is null or is_premium = ''",
     'expected_result': 0,
     'descr': "null values"}
  
]   
    
start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

create_tables_task = PostgresOperator(
    task_id='create_tables',
    dag=dag,
    postgres_conn_id=getRedshiftConnId(),
    sql='create_tables.sql'
)

stage_listings_to_redshift = StageToRedshiftOperator(
    task_id='Stage_listing',
    dag=dag,
    provide_context=True,
    redshift_conn_id=getRedshiftConnId(),
    table_name='staging_listing',
    s3_bucket=getS3Bucket(),
    s3_path='listings.csv',
    aws_key=AWS_KEY,
    aws_secret=AWS_SECRET,
  
)

stage_calendar_to_redshift = StageToRedshiftOperator(
    task_id='Stage_calendar',
    dag=dag,
    provide_context=True,
    redshift_conn_id=getRedshiftConnId(),
    table_name='staging_calendar',
    s3_bucket=getS3Bucket(),
    s3_path='calendar.csv',
    aws_key=AWS_KEY,
    aws_secret=AWS_SECRET,  
)

stage_review_to_redshift = StageToRedshiftOperator(
    task_id='Stage_review',
    dag=dag,
    provide_context=True,
    redshift_conn_id=getRedshiftConnId(),
    table_name='staging_review',
    s3_bucket=getS3Bucket(),
    s3_path='reviews.csv',
    aws_key=AWS_KEY,
    aws_secret=AWS_SECRET,
)

load_dimension_listing = LoadDimensionOperator(
    task_id='Load_listing_dim_table',
    dag=dag,
    provide_context=True,
    redshift_conn_id=getRedshiftConnId(),
    table_name='dim_listing',
    insert_sql=SqlQueries.dim_listing_table_insert,
    truncate_table='Y'
)

load_dimension_host = LoadDimensionOperator(
    task_id='Load_dim_host_table',
    dag=dag,
    provide_context=True,
    redshift_conn_id=getRedshiftConnId(),
    table_name='dim_host',
    insert_sql=SqlQueries.dim_host_table_insert,
    truncate_table='Y'
)

load_dimension_district = LoadDimensionOperator(
    task_id='Load_dim_district_table',
    dag=dag,
    provide_context=True,
    redshift_conn_id=getRedshiftConnId(),
    table_name='dim_district',
    insert_sql=SqlQueries.dim_district_table_insert,
    truncate_table='Y'
)

load_dimension_neighbourhood = LoadDimensionOperator(
    task_id='Load_dim_neighbourhood_table',
    dag=dag,
    provide_context=True,
    redshift_conn_id=getRedshiftConnId(),
    table_name='dim_neighbourhood',
    insert_sql=SqlQueries.dim_neighbourhood_table_insert,
    truncate_table='Y'
)

load_dimension_property = LoadDimensionOperator(
    task_id='Load_dim_property_table',
    dag=dag,
    provide_context=True,
    redshift_conn_id=getRedshiftConnId(),
    table_name='dim_property',
    insert_sql=SqlQueries.dim_property_table_insert,
    truncate_table='Y'
)

load_dimension_review = LoadDimensionOperator(
    task_id='Load_dim_review_table',
    dag=dag,
    provide_context=True,
    redshift_conn_id=getRedshiftConnId(),
    table_name='dim_review',
    insert_sql=SqlQueries.dim_review_table_insert,
    truncate_table='Y'
)

load_dimension_date = LoadDimensionOperator(
    task_id='Load_dim_date_table',
    dag=dag,
    provide_context=True,
    redshift_conn_id=getRedshiftConnId(),
    table_name='dim_date',
    insert_sql=SqlQueries.dim_date_table_insert,
    truncate_table='Y'
)


load_fact_neighbourhood = LoadFactOperator(
    task_id='Load_fact_neighbourhood',
    dag=dag,
    provide_context=True,
    redshift_conn_id=getRedshiftConnId(),
    table_name='fact_neighbourhood',
    insert_sql=SqlQueries.fact_neighbourhood_insert
)

load_fact_listing_table = LoadFactOperator(
    task_id='Load_fact_listing_table',
    dag=dag,
    provide_context=True,
    redshift_conn_id=getRedshiftConnId(),
    table_name='fact_listing',
    insert_sql=SqlQueries.fact_listing_table_insert
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    provide_context=True,
    redshift_conn_id=getRedshiftConnId(),
    checks=dq_checks
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)
    
start_operator >> \
create_tables_task >> [stage_listings_to_redshift,
                       stage_calendar_to_redshift, stage_review_to_redshift, load_dimension_date ]

stage_listings_to_redshift >> [load_dimension_district ,
                                 load_dimension_host,
                                 load_dimension_property] 

stage_review_to_redshift >> load_dimension_review

load_dimension_district >>  load_dimension_neighbourhood

stage_calendar_to_redshift >> load_dimension_listing 
load_dimension_neighbourhood >> load_dimension_listing 

load_dimension_listing  >> [load_fact_neighbourhood, load_fact_listing_table]

load_dimension_neighbourhood >> [ load_fact_neighbourhood, load_fact_listing_table ]
load_dimension_host >> load_fact_listing_table
load_dimension_review >> load_fact_listing_table
load_fact_neighbourhood >> run_quality_checks 
load_fact_listing_table >> run_quality_checks 
run_quality_checks >> end_operator                   
    