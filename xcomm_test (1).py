import json
import os
import logging



def xcomm_test(**kwargs):
    # Use the task instance (ti) to push XCom
    kwargs['ti'].xcom_push(key='my_custom_key', value='custom_value')
