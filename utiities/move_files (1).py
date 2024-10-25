import os
import json
import logging
from datetime import datetime

from azure.storage.blob import BlobServiceClient
from airflow.exceptions import AirflowFailException

def python_callable_move_files(**ftp_params):
    pulled_value = ftp_params['ti'].xcom_pull(task_ids='send_scheduler_email', key='my_custom_key')
    print(f"Pulled Value from XCom: {pulled_value}")
    print(f"THis is the object passed : {ftp_params}")
    # Get the current working directory
    current_directory = os.getcwd()

    # Construct the path to the 'dags' directory
    app_config_path = os.path.join(current_directory, 'dags','utilities', 'appsettings.json')

    with open(app_config_path, 'r') as file:
        json_obj = json.load(file)
    # environment variables to be pass
    client_name = "adminsure"
    environ_name = "dev"
    ftp_request_params = json_obj["clientSpecificDetails"][client_name][environ_name]
    file_counter = 0
    str_dir_path = ""

    try:
        if ftp_params["SourceLocation"].strip() == "":
            logging.error("Job Execution Failed Since The Source Location Was Empty")
            raise AirflowFailException("Job Execution Failed Since The Source Location Was Empty")

        elif ftp_params["TargetLocation"].strip() == "":
            logging.error("Job Execution Failed Since The Target Location Was Empty")
            raise AirflowFailException("Job Execution Failed Since The Target Location Was Empty")

        else:
            arr_source_container = ftp_params["SourceLocation"].split("/")
            arr_destination_container = ftp_params["TargetLocation"].split("/")

            for bloburiitem in arr_source_container[4:]:
                str_dir_path = f"{str_dir_path}/{bloburiitem}" if str_dir_path else bloburiitem

            for bloburiitem in arr_destination_container[4:]:
                str_destination_dir_path = f"{str_destination_dir_path}/{bloburiitem}" if file_counter >= 4 else bloburiitem
                file_counter += 1

            # Azure Blob client setup
            blob_service_client = BlobServiceClient.from_connection_string(ftp_request_params['azureBlobDetails']['connectionString'])
            container_client = blob_service_client.get_container_client(arr_source_container[3])
            dest_container_client = blob_service_client.get_container_client(arr_destination_container[3])

            # Check for file naming convention
            if "*" in ftp_params['FileNamingConvention']:
                file_prefix = ftp_params["FileNamingConvention"].split("*")
                start_file_name = file_prefix[0]
                file_extension_to_match = file_prefix[-1].replace(".", "")
                if start_file_name.strip():
                    str_dir_path += start_file_name
            else:
                file_prefix = ftp_params["FileNamingConvention"].split(".")
                start_file_name = file_prefix[0]
                file_extension_to_match = file_prefix[-1]
                str_dir_path += f"/{start_file_name}.{file_extension_to_match}"

            # Iterate over blobs and move files
            curr_datetime = datetime.now().strftime("%Y%m%d_%H%M")
            for blob_item in container_client.list_blobs(name_starts_with=str_dir_path):
                blob_client = container_client.get_blob_client(blob_item.name)
                blob_name = blob_item.name.split("/")[-1]
                blob_name_without_ext, blob_extension = blob_name.rsplit(".", 1)
                tranfer_file_name = f"{blob_name_without_ext}_{curr_datetime}.{blob_extension}"

                if not file_extension_to_match or file_extension_to_match.lower() == blob_extension.lower():
                    dest_blob_client = dest_container_client.get_blob_client(
                        f"{str_destination_dir_path}/{tranfer_file_name}")
                    dest_blob_client.start_copy_from_url(blob_client.url)
                    blob_client.delete_blob()
            logging.info("ftp_file_movement task completed")

    except Exception as ex:
        logging.error(f"Failure: {ex}")
        raise AirflowFailException("Something went wrong. Please contact the administrator.")

    logging.info("ftp/sftp_file_movement task completed")
