import json
import os
import logging
import time
from base64 import b64encode
from io import BytesIO
import sendgrid
from sendgrid.helpers.mail import Mail, Attachment
from airflow.exceptions import AirflowFailException
from sendgrid.helpers.mail import Mail,Email,To,Cc,Bcc

def notify_email(context):
    dag_id = context['dag'].dag_id
    task_id = context['task_instance'].task_id
    exception = context['exception']
	# Pass this to list from context object
    to = context['dag'].default_args.get('email_recipients', [])
    subject = f"Airflow alert: {task_id} in {dag_id} failed"
    html_content = f"""
        <p>Task <strong>{task_id}</strong> in DAG <strong>{dag_id}</strong> failed.</p>
        <p>Error: {exception}</p>
        """
    # Get the current working directory
    current_directory = os.getcwd()

    # Construct the path to the 'dags' directory
    app_config_path = os.path.join(current_directory, 'dags','utilities', 'appsettings.json')

    with open(app_config_path, 'r') as file:
        json_obj = json.load(file)
    # environment variables to be pass
    client_name = "adminsure"
    environ_name = "dev"
    email_request = json_obj["clientSpecificDetails"][client_name][environ_name]
    msg = Mail(
        from_email=email_request['sendGridEmailSettings']['fromMail'],
        to_emails=['krisnand2k@gmail.com'],
        subject=f"Airflow alert: {task_id} in {dag_id} failed",
        html_content=html_content
        )
    client = sendgrid.SendGridAPIClient(email_request['sendGridEmailSettings']['apiKey'])
    client.send(msg)