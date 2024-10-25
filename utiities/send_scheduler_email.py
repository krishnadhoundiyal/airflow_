import json
import os
import logging
import time
from base64 import b64encode
from io import BytesIO
import sendgrid
from azure.storage.blob import BlobServiceClient
from sendgrid.helpers.mail import Mail, Attachment
from airflow.exceptions import AirflowFailException
from sendgrid.helpers.mail import Mail,Email,To,Cc,Bcc


def find_duplicate(email, email_list):
    return any(e == email for e in email_list)


def send_scheduler_email(**email_param):
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
    try:
        if not email_request['sendGridEmailSettings']['apiKey'].strip():
            logging.error("Error: Email Key is not available")
            raise AirflowFailException("Error: Email Key is not available")

        if not email_request['sendGridEmailSettings']['fromMail'].strip():
            logging.error("Error: From Address is not available")
            raise AirflowFailException("Error: From Address is not available")

        client = sendgrid.SendGridAPIClient(email_request['sendGridEmailSettings']['apiKey'])
        tos_list = [to.strip() for to in email_param['ToEmail'].split(',') if to.strip()]
        if not tos_list:
            logging.error("Error: To Address is not available")
            raise AirflowFailException("Error: To Address is not available")

        if not email_param['Subject'].strip():
            if not tos_list:
                logging.error("Error: Email Subject is not available")
                raise AirflowFailException("Error: Email Subject is not available")

        ccs_list = []
        if email_param['CCEmail']:
            ccs_list = [cc.strip() for cc in email_param['CCEmail'].split(',') if
                        cc.strip() and not find_duplicate(cc.strip(), tos_list)]

        bcc_list = []
        if email_param['BCCEmail']:
            bcc_list = [bcc.strip() for bcc in email_param['BCCEmail'].split(',') if
                        bcc.strip() and not find_duplicate(bcc.strip(), ccs_list)]

        msg = Mail(
            from_email=email_request['sendGridEmailSettings']['fromMail'],
            to_emails=tos_list,
            subject=email_param['Subject'],
            plain_text_content=email_param.get('Body', ' ')
        )

        # Add CCs if present
        if ccs_list:
            for cc in ccs_list:
                msg.add_cc(Cc(cc))
        # Add BCCs if present
        if bcc_list:
            for bcc in bcc_list:
                msg.add_bcc(Bcc(bcc))

        if email_param.get('Attachment'):
            blob_service_client = BlobServiceClient.from_connection_string(email_request['azureBlobDetails']['connectionString'])
            container_client = blob_service_client.get_container_client(
                email_request['azureBlobDetails']['relevantContainer'])
            blob_client = container_client.get_blob_client(email_param['Attachment'])

            with BytesIO() as memory_stream:
                blob_client.download_blob().download_to_stream(memory_stream)
                memory_stream.seek(0)
                file_bytes = memory_stream.read()
                msg.add_attachment(Attachment(b64encode(file_bytes).decode(), email_param['Attachment']))

        email_response = client.send(msg)
        if email_response.status_code >= 200 and email_response.status_code < 300:
            logging.info("Success:Email Sent")
        else:
            logging.error("Error: Sending Email")
            raise AirflowFailException("Error: Sending Email")

    except Exception as ex:
        logging.error(f"Error: Sending Email: {repr(ex)}")
        raise AirflowFailException("Error: Sending Email")
