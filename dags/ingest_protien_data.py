import glob
import os
from datetime import datetime, timedelta

import pendulum
from airflow import DAG
from airflow.exceptions import AirflowNotFoundException
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.python_operator import PythonOperator

from lib.neo4j import IngestIntoNeo4jOperator

documentation_markdown = """
### General Information
This pipeline process the data from a a source location into NEO4J database
"""

DAG_ID = "ingest_protien_data"

SOURCE_DATA_ROOT = f"dags/data/"

default_args = {
    "depends_on_past": False,
    "start_date": datetime(2023, 1, 3),
}

with DAG(
        dag_id=DAG_ID,
        max_active_runs=1,
        catchup=False,
        schedule_interval=None,
        template_searchpath=["dags/config"],
        doc_md=documentation_markdown,
        default_args=default_args
) as dag:
    def check_input_file(success_condition: str, failure_condition: str, **kwargs):
        directory_path = SOURCE_DATA_ROOT
        pattern = "*.xml"
        if os.path.isdir(directory_path):
            files = glob.glob(os.path.join(directory_path, pattern))
            if len(files) > 0:
                return success_condition
            else:
                return failure_condition
        else:
            raise AirflowNotFoundException(f"directory {directory_path} not found")


    check_input_file = BranchPythonOperator(
        task_id="check_for_files",
        python_callable=check_input_file,
        provide_context=True,
        op_kwargs={
            "success_condition": "file_exists",
            "failure_condition": "file_not_exists"
        }
    )

    file_not_exists = DummyOperator(task_id='file_not_exists')

    file_exists = DummyOperator(task_id='file_exists')


    def add_to_queue(**context):
        directory_path = SOURCE_DATA_ROOT
        pattern = "*.xml"
        files = glob.glob(os.path.join(directory_path, pattern))
        # This is a standalone method for pushing into a queue. In a wider scalable and decoupled implementation, we should push this to a queuing system.
        context['ti'].xcom_push(key='file_list', value=files)


    add_to_queue = PythonOperator(task_id='add_to_queue', python_callable=add_to_queue)

    ingest_data = IngestIntoNeo4jOperator(
        task_id='ingest_into_neo4j',
        neo4j_conn='',
        queue_name="{{ task_instance.xcom_pull(task_ids='add_to_queue', key='file_list')}}"
    )

    end_run = DummyOperator(task_id='end_run')

    check_input_file >> [file_exists, file_not_exists]
    file_exists >> add_to_queue >> ingest_data >> end_run
    file_not_exists >> end_run
