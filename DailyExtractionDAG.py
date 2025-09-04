from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from FundExtractor import run_daily, run_kuvera
from AzureDataExtractor import hit_data_factory_api

with DAG(
    dag_id='nav_daily_runner',
    start_date=datetime(2023, 1, 1),
    schedule_interval='@daily',
    catchup=False
) as dag:

    run_pipeline = PythonOperator(
        task_id='run_daily_pipeline',
        python_callable=run_daily,
        op_kwargs={
            'config_path': 'run_time_config.json',
            'search_new': False
        }
    )

    run_pipeline_1 = PythonOperator(
        task_id='kuvera_1',
        python_callable=run_kuvera,
        op_kwargs={
            'operation': 'isinDivReinvestment'
        }
    )

    run_pipeline_2 = PythonOperator(
        task_id='kuvera_2',
        python_callable=run_kuvera,
        op_kwargs={
            'operation': 'isinGrowth'
        }
    )

    data_factory_pipeline = PythonOperator(
        task_id='data_factory_pipeline',
        python_callable=hit_data_factory_api,
        op_kwargs={
            'factory_name': 'mf-fund-pipeline-factory' ,
            'pipeline_name': 'Daily_Mutual_Fund_Pipeline'
        }
    )

    run_pipeline >> [run_pipeline_1, run_pipeline_2] >> data_factory_pipeline
