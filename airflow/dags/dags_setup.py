from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from ETL_Cassandra_S3 import *
from create_redshift import *
from fake_data import *
default_args = {
    'owner' : 'tmph2003'
}

with DAG (
    dag_id='ETL_cassandra_s3_dag',
    default_args=default_args,
    start_date=datetime.datetime(2023, 9, 3),
    schedule_interval='*/5 * * * *'
) as dag :
    fake_data = PythonOperator(
        task_id = 'fake_data',
        python_callable = generating_dummy_data,
        op_args=[random.randint(1,20)]
    )

    Extract = PythonOperator(
        task_id = 'extract_data_from_cassandra',
        python_callable = extract_data_from_cassandra,
    )

    Transform = PythonOperator(
        task_id = 'Transform_data',
        python_callable = process_result,
        op_args=[Extract.output]
    )

    Load = PythonOperator(
        task_id = 'Load_data_to_s3',
        python_callable = write_data_to_s3,
        op_args=[Transform.output]
    )

    create_redshift = PythonOperator (
        task_id = 'create_redshift_schema_and_copy_data_to_redshift',
        python_callable = create_redshift_schema
    )
    fake_data >> Extract >> Transform >> Load >> create_redshift