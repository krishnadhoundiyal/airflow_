import pyodbc
import subprocess
import json
import os
import logging
import time
from base64 import b64encode
import ftplib
import paramiko
from azure.storage.blob import BlobServiceClient
#from paramiko import SFTPClient, Transport
from airflow.exceptions import AirflowFailException
def python_callable_sftp_export(**ftp_params):
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
    str_file_name = ''
    file_counter = 0
    try:
        # Check for required parameters

        if not ftp_params['SourceLocation']:
            logging.error("Source Location For File Not Available")
            raise AirflowFailException("Source Location For File Not Available")
        elif not ftp_params['HostName']:
            logging.error("Host Name Not Available")
            raise AirflowFailException("Host Name Not Available")
        elif not ftp_params['Password']:
            logging.error("Password To Connect To Host Not Available")
            raise AirflowFailException("Password To Connect To Host Not Available")
        else:
            decoded_url = ftp_params['SourceLocation']
            arr_file_name = decoded_url.split("/")

            # Determine the SFTP port
            sftp_port_to_use = 22
            if ftp_params.get('Port'):
                sftp_port_to_use = int(ftp_params.get('Port', 22))
            else:
                ftp_params['Port'] = 22

            # Construct file path
            for bloburiitem in arr_file_name[3:]:
                str_file_name = f"{str_file_name}/{bloburiitem}" if str_file_name else bloburiitem
                file_counter += 1

            folder_name = str_file_name
            prefix, new_str = '', ''

            # Handle file naming convention
            if '*' in ftp_params['FileNamingConvention']:
                file_prefix = ftp_params['FileNamingConvention'].split('*')
                prefix = f"{folder_name}/{file_prefix[0]}"
                new_str = file_prefix[1].replace('.', '')
            else:
                file_prefix = ftp_params['FileNamingConvention'].split('.')
                prefix = f"{folder_name}/{file_prefix[0]}"
                new_str = file_prefix[1]

            # Blob storage client setup
            blob_service_client = BlobServiceClient.from_connection_string(ftp_request_params['azureBlobDetails']['connectionString'])
            container_client = blob_service_client.get_container_client(ftp_request_params['azureBlobDetails']['relevantContainer'])

            # Loop over blobs and process each file
            for blob_item in container_client.list_blobs(name_starts_with=prefix):
                blob_client = container_client.get_blob_client(blob_item)
                blob_name = blob_client.blob_name
                file_extension = blob_name.split(".")[-1]

                if file_extension == new_str:
                    target_name = os.path.join("adminsure", blob_name.split("/")[-1])
                    blob_client.download_blob().download_to_path(target_name)

                    # FTP/SFTP handling
                    if ftp_params['Protocol'].lower() != "sftp":
                        upload_to_ftp(ftp_params, target_name, blob_name)
                    else:
                        upload_to_sftp(ftp_params, sftp_port_to_use, target_name, blob_name)

                    # Archive file after copying if needed
                    if ftp_params['DeleteFilesFromSourceSFTPLocation'] == 1:
                        archive_blob_client = container_client.get_blob_client(f"archive/{blob_name}")
                        archive_blob_client.start_copy_from_url(blob_client.url)
                        blob_client.delete_blob()
                    logging.info("upload_file_to_sftp_server_async task completed")

    except Exception as ex:
        logging.error(f"Error: {ex}")
        raise AirflowFailException("Error In Fetching Data For The Job From")

    logging.info("upload_file_to_sftp_server_async task completed")


def upload_to_ftp(ftp_params, target_name, blob_name):
    ftp_host = f"ftp://{ftp_params['HostName']}:{ftp_params['Port']}"
    ftp_server_with_path = os.path.join(ftp_params['DestinationLocation'], blob_name)
    with ftplib.FTP(ftp_host) as ftp:
        ftp.login(ftp_params['UserName'], ftp_params['Password'])
        with open(target_name, 'rb') as file:
            ftp.storbinary(f"STOR {ftp_server_with_path}", file)


def upload_to_sftp(ftp_params, sftp_port, target_name, blob_name):
    transport = Transport((ftp_params['HostName'], sftp_port))
    transport.connect(username=ftp_params['UserName'], password=ftp_params['Password'])
    sftp = SFTPClient.from_transport(transport)

    try:
        sftp.chdir(ftp_params['DestinationLocation'])
        with open(target_name, 'rb') as file:
            sftp.putfo(file, blob_name)
    finally:
        sftp.close()
        transport.close()


def python_callable_sftp_import(**ftp_params):
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
    str_file_name = ''
    num_of_files = 0
    file_counter = 0
    try:
        sftp_port = 22 if not ftp_params.get('Port') else int(ftp_params.get('Port'))
        decoded_url = ftp_params.get('DestinationLocation')
        file_name_parts = decoded_url.split('/')
        target_name = ''
        file_name_expression = ''
        new_str = ''

        # File naming convention handling
        if '*' in ftp_params.get('FileNamingConvention'):
            file_prefix = ftp_params.get('FileNamingConvention').split('*')
            new_str = file_prefix[1].replace('.', '')
            file_name_expression = file_prefix[0].replace('_', '')
        else:
            file_prefix = ftp_params.get('FileNamingConvention').split('.')
            new_str = file_prefix[1]
            file_name_expression = file_prefix[0]

        # FTP/SFTP operations
        if ftp_params.get('Protocol').lower() != 'sftp':
            ftp_host = f"ftp://{ftp_params.get('HostName')}:{sftp_port}"
            ftp_server_with_path = ftp_params.get('SourceLocation')

            ftps = FTP_TLS()
            ftps.connect(ftp_params.get('HostName'), sftp_port)
            ftps.login(ftp_params.get('UserName'), ftp_params.get('Password'))
            ftps.prot_p()

            file_list = ftps.nlst(ftp_server_with_path)

            for file_name in file_list:
                if not file_name.startswith(file_name_expression):
                    continue

                local_file_path = os.path.join(ftp_request_params.get('rootPath'), file_name)
                with open(local_file_path, 'wb') as local_file:
                    ftps.retrbinary(f"RETR {ftp_server_with_path}/{file_name}", local_file.write)

                # Upload to Blob
                blob_client = BlobServiceClient.from_connection_string(ftp_request_params['azureBlobDetails']['connectionString'],container_name=client_name,blob_name=file_name)
                with open(local_file_path, 'rb') as data:
                    blob_client.upload_blob(data)
                num_of_files += 1

                # Optionally delete file from FTP
                if ftp_params.get('DeleteFilesFromSource') == 1:
                    ftps.delete(f"{ftp_server_with_path}/{file_name}")

        else:
            with paramiko.SSHClient() as client:
                client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                client.connect(ftp_params.get('HostName'), port=sftp_port, username=ftp_params.get('UserName'),
                               password=ftp_params.get('Password'))

                sftp = client.open_sftp()
                file_list = sftp.listdir(ftp_params.get('SourceLocation'))

                for remote_file in file_list:
                    if remote_file.startswith(file_name_expression):
                        remote_file_path = os.path.join(ftp_params.get('SourceLocation'), remote_file)
                        local_file_path = os.path.join(client_name, remote_file)

                        # Download the file
                        sftp.get(remote_file_path, local_file_path)

                        # Upload to Blob
                        blob_client = BlobServiceClient.from_connection_string(ftp_request_params['azureBlobDetails']['connectionString'],container_name=client_name,blob_name=remote_file)
                        with open(local_file_path, 'rb') as data:
                            blob_client.upload_blob(data)
                        num_of_files += 1

                        # Optionally delete file from SFTP
                        if ftp_params.get('DeleteFilesFromSource') == 1:
                            sftp.remove(remote_file_path)

            logging.info(f"Total Number of Files Imported: {num_of_files}")

    except Exception as ex:
        logging.error(f"Error: {ex}")
        raise AirflowFailException("Error In downloading Data For The Job From sftp/ftp server")

    logging.info(f"Total Number of Files Imported: {num_of_files}")

def test_mssql_connection():
    # Define your connection parameters
	server = 'cmsdev1.database.windows.net'  # e.g., 'localhost' or '192.168.1.1'
	database = 'kleardb_dev1'  # Your database name
	username = 'devuser'  # Your username
	password = 'D3v@cce55'  # Your password
	driver = '{ODBC Driver 17 for SQL Server}'   # ODBC Driver for SQL Server
	result = subprocess.run(['pip', 'freeze'], stdout=subprocess.PIPE, text=True, check=True)
	connection_string = f'DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password}'
	connection = pyodbc.connect(connection_string)
	cursor = connection.cursor()
	cursor.execute("select * from sch.schjob where SchJobName like '%Froi%';")
	rows = cursor.fetchall()
	for row in rows:
		print(row)
	# Print the output of pip freeze
	print(result.stdout)

