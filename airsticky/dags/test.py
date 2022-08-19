from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.snowflake.transfers.s3_to_snowflake import S3ToSnowflakeOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator


from datetime import datetime, timedelta
import os
import requests

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG('snowsqls_jrimba',
         start_date=datetime(2022, 8, 1),
         max_active_runs=1,
         schedule_interval='@monthly',
         default_args=default_args,
         catchup=False,
         template_searchpath='/usr/local/airflow/include'
         ) as dag:

    Create_Schemas = SnowflakeOperator(
             task_id='0_0_Create_Schemas',
             snowflake_conn_id='snowflake',
             sql='0_Schema_DB/0_0_Create_Schemas.sql'
         )
    Create_file_format1 = SnowflakeOperator(
             task_id='0_1_Create_file_format1',
             snowflake_conn_id='snowflake',
             sql='0_Schema_DB/0_1_Create_file_format1.sql'
         )
    Create_file_format2_compress = SnowflakeOperator(
             task_id='0_2_Create_file_format2_compress',
             snowflake_conn_id='snowflake',
             sql='0_Schema_DB/0_2_Create_file_format2_compress.sql'
         )
    Create_and_import_Aisles = SnowflakeOperator(
             task_id='0_3_Create_and_import_Aisles',
             snowflake_conn_id='snowflake',
             sql='0_Schema_DB/0_3_Create_and_import_Aisles.sql'
         )    
    Create_and_import_Departments = SnowflakeOperator(
             task_id='0_4_Create_and_import_Departments',
             snowflake_conn_id='snowflake',
             sql='0_Schema_DB/0_4_Create_and_import_Departments.sql'
         )        
    Create_and_import_Orders = SnowflakeOperator(
             task_id='0_5_Create_and_import_Orders',
             snowflake_conn_id='snowflake',
             sql='0_Schema_DB/0_5_Create_and_import_Orders.sql'
         )            
    Create_and_import_products = SnowflakeOperator(
             task_id='0_6_Create_and_import_products',
             snowflake_conn_id='snowflake',
             sql='0_Schema_DB/0_6_Create_and_import_products.sql'
         )    
    Create_and_import_order_products_prior = SnowflakeOperator(
             task_id='0_7_Create_and_import_order_products_prior',
             snowflake_conn_id='snowflake',
             sql='0_Schema_DB/0_7_Create_and_import_order_products_prior.sql'
         )    
    Create_and_import_order_products_train = SnowflakeOperator(
             task_id='0_8_Create_and_import_order_products_train',
             snowflake_conn_id='snowflake',
             sql='0_Schema_DB/0_8_Create_and_import_order_products_train.sql'
         )        
    Generate_table_orders_prior = SnowflakeOperator(
             task_id='1_1_Generate_table_orders_prior',
             snowflake_conn_id='snowflake',
             sql='1_Schema_ODS/1_1_Generate_table_orders_prior.sql'
         )
    '''
    Generate_table_orders_train = SnowflakeOperator(
             task_id='1_2_Generate_table_orders_train',
             snowflake_conn_id='snowflake',
             sql='1_Schema_ODS/1_2_Generate_table_orders_train.sql'
         )    
    '''        
    Generate_user_feature_1 = SnowflakeOperator(
             task_id='2_1_Generate_user_feature_1',
             snowflake_conn_id='snowflake',
             sql='2_Schema_DWM/2_1_Generate_user_feature_1.sql'
         )  
           
    Generate_user_feature_2 = SnowflakeOperator(
             task_id='2_2_Generate_user_feature_2',
             snowflake_conn_id='snowflake',
             sql='2_Schema_DWM/2_2_Generate_user_feature_2.sql'
         )
    Generate_up_features = SnowflakeOperator(
             task_id='2_3_Generate_up_features',
             snowflake_conn_id='snowflake',
             sql='2_Schema_DWM/2_3_Generate_up_features.sql'
         )
                          
    Generate_prd_feature_1 = SnowflakeOperator(
             task_id='2_4_Generate_prd_feature_1',
             snowflake_conn_id='snowflake',
             sql='2_Schema_DWM/2_4_Generate_prd_feature_1.sql'
         )
    
    SageMakerIutput = SnowflakeOperator(
             task_id='3_1_SageMakerInput',
             snowflake_conn_id='snowflake',
             sql='3_Schema_DWS/3_1_SageMakerOutput.sql'
         )    
    SaveFileToS3 = SnowflakeOperator(
             task_id='3_2_SaveFileToS3',
             snowflake_conn_id='snowflake',
             sql='3_Schema_DWS/3_2_SaveFileToS3.sql'
         )    
        
    

                 
    Create_Schemas >> [Create_file_format1,Create_file_format2_compress]
    Create_file_format1>> [Create_and_import_Aisles,Create_and_import_Departments,Create_and_import_Orders, Create_and_import_products]
    Create_file_format2_compress >> [Create_and_import_order_products_prior,Create_and_import_order_products_train]
    [Create_and_import_Orders, Create_and_import_order_products_prior] >> Generate_table_orders_prior
    Create_and_import_Orders >> Generate_user_feature_1
    Generate_table_orders_prior >> [Generate_user_feature_2,Generate_up_features, Generate_prd_feature_1]
    [Generate_user_feature_2,Generate_up_features, Generate_prd_feature_1,Generate_user_feature_1 ] >> SageMakerIutput
    SageMakerIutput >> SaveFileToS3
                                                                              