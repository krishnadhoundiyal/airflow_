import sys
import os
import pyodbc

# Add the 'utilities' folder to the Python path so we can import utility functions
sys.path.append(os.path.join(os.path.dirname(__file__), 'utilities'))

# Dynamically import the Python callable based on schServiceCategoryName
from importlib import import_module
python_callable_module = import_module("utilities.sftp_import_utilities")
python_callable_func_sftp_import = getattr(python_callable_module, "python_callable_sftp_import")

python_callable_func_sftp_import_mssql = getattr(python_callable_module, "test_mssql_connection")

python_callable_module = import_module("utilities.sftp_import_utilities")
python_callable_func_sftp_export = getattr(python_callable_module, "python_callable_sftp_export")

python_callable_module_mail = import_module("utilities.send_scheduler_email")
python_callable_func_mail = getattr(python_callable_module_mail, "send_scheduler_email")

# python_callable_module_scheduler = import_module("utilities.skip_scheduler_if_expression")
# python_callable_scheduler_check = getattr(python_callable_module_scheduler, "skip_if_not_nth_cycle")
#
# python_callable_module_check_task = import_module("utilities.check_most_recent_task_state")
# python_callable_check_task = getattr(python_callable_module_check_task, "check_most_recent_task_state")

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago
from datetime import timedelta, datetime

# Define the default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime.strptime("2024-10-15", '%Y-%m-%d'),
    'end_date': datetime.strptime("2026-12-18", '%Y-%m-%d')
}
# Define the DAG
dag = DAG(dag_id="workflow_6", default_args=default_args, schedule_interval="13 01 * * 1,2,6", catchup=False)



# Define the Python task using the dynamically imported function
python_task = PythonOperator(
    task_id="send_scheduler_sftp",
    python_callable=python_callable_func_sftp_import_mssql,
    op_kwargs={
        "Protocol": "SFTP",
        "HostName": "adminsuresftpprd.blob.core.windows.net",
        "Port": "22",
        "UserName": "adminsuresftpprd.adminsuresftpadmin",
        "Password": "vhnchqOe301ic2I2K9oVNx5VTrGxrnOT",
        "SourceLocation": "/Documents/OTHER",
        "DestinationLocation": "https://adminsureklearaiprod.blob.core.windows.net/prd/inbound",
        "FileNamingConvention": "*.*",
        "DeleteFilesFromSourceSFTPLocation": "0",
    },
    provide_context=True,
    dag=dag
)


# Define the task flow

python_task

