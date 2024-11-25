from airflow import DAG
from datetime import datetime
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import PythonOperator
from etl.source_to_stage import *
from etl.stage_to_nds import *

with DAG(
    dag_id = 'ETL_AQI',
    start_date=datetime(2024, 11, 20),
    schedule_interval='0 22 * * *',
    catchup=False,
) as dag:
    with TaskGroup("source_to_stage") as source_to_stage_group:
        with TaskGroup("load_into_aqi_stage") as load_into_stage1:
            set_cet_state_aqi_task = PythonOperator(
                task_id='set_cet_state_aqi',
                python_callable=set_cet,
                op_kwargs={'table_name': 'state_aqi_stage'}
            )
            truncate_table_state_aqi_stage_task = PythonOperator(
                task_id='truncate_table_state_aqi_stage',
                python_callable=truncate_table,
                op_kwargs={'table_name': 'state_aqi_stage'}
            )
            get_metadata_state_aqi_task = PythonOperator(
                task_id='get_metadata_state_aqi',
                python_callable=get_metadata,
                op_kwargs={'table_name': 'state_aqi_stage'}
            )
            process_aqi_files_task = PythonOperator(
                task_id='process_aqi_files',
                python_callable=process_aqi_files,
                op_kwargs={'table_name': 'state_aqi_stage'}
            )
            set_lset_state_aqi_task = PythonOperator(
                task_id='set_lset_state_aqi',
                python_callable=set_lset,
                op_kwargs={'table_name': 'state_aqi_stage'}
            )
            #Luồng hoạt động của quá trình ETL cho bảng state_aqi_stage
            set_cet_state_aqi_task >> truncate_table_state_aqi_stage_task >> get_metadata_state_aqi_task >> process_aqi_files_task >> set_lset_state_aqi_task    

        with TaskGroup("load_into_counties_stage") as load_into_stage2:
            set_cet_us_counties_task = PythonOperator(
                task_id='set_cet_us_counties',
                python_callable=set_cet,
                op_kwargs={'table_name': 'us_counties_stage'}
            )
            truncate_table_us_counties_stage_task = PythonOperator(
                task_id='truncate_table_us_counties_stage',
                python_callable=truncate_table,
                op_kwargs={'table_name': 'us_counties_stage'}
            )
            process_counties_file_task = PythonOperator(
                task_id='process_counties_file',
                python_callable=process_counties_file,
                op_kwargs={'table_name': 'us_counties_stage'}
            )
            set_lset_us_counties_task = PythonOperator(
                task_id='set_lset_us_counties',
                python_callable=set_lset,
                op_kwargs={'table_name': 'us_counties_stage'}
            )
            #Luồng hoạt động của quá trình ETL cho bảng us_counties_stage
            set_cet_us_counties_task >> truncate_table_us_counties_stage_task >> process_counties_file_task >> set_lset_us_counties_task

    with TaskGroup('stage_to_nds') as stage_to_nds_group:
        get_merged_state_data_task = PythonOperator(
            task_id='get_merged_state_data',
            python_callable=get_merged_state_data
        )
        get_merged_county_data_task = PythonOperator(
            task_id='get_merged_county_data',
            python_callable=get_merged_county_data
        )
        get_merged_measurement_data_task = PythonOperator(
            task_id='get_merged_measurement_data',
            python_callable=get_merged_measurement_data
        )
        #Luồng hoạt động của quá trình ETL cho các bảng trong NDS
        get_merged_state_data_task >> get_merged_county_data_task >> get_merged_measurement_data_task

    #Luồng hoạt động tổng    
    source_to_stage_group >> stage_to_nds_group
