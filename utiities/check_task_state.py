import psycopg2
from airflow.utils.state import State
from airflow.hooks.base_hook import BaseHook

def check_most_recent_task_state(dag_id, task_id):
    """
    This function checks the most recent state of a task in the Airflow metadata
    database and determines whether it was successful or failed.
    
    :param dag_id: DAG ID of the task to check
    :param task_id: Task ID to check
    :return: 'trigger_on_success' if the task succeeded, 'trigger_on_failure' otherwise
    """

    # Fetch the connection details from Airflow
    connection = BaseHook.get_connection('airflow_meta_db')  # This should match the connection set up in Airflow UI

    # Establish connection to the Airflow metadata PostgreSQL database
    conn = psycopg2.connect(
        dbname=connection.schema,
        user=connection.login,
        password=connection.password,
        host=connection.host,
        port=connection.port
    )

    # Create a cursor for database operations
    cursor = conn.cursor()

    # SQL query to fetch the most recent state of the task from the task_instance table
    query = """
    SELECT state
    FROM task_instance
    WHERE dag_id = %s
      AND task_id = %s
    ORDER BY start_date DESC
    LIMIT 1
    """

    # Execute the query
    cursor.execute(query, (dag_id, task_id))

    # Fetch the result
    result = cursor.fetchone()

    # Close the cursor and connection
    cursor.close()
    conn.close()

    # Check if the result exists and determine whether it was successful
    if result and result[0] == State.SUCCESS:
        return 'trigger_on_success'
    else:
        return 'trigger_on_failure'

