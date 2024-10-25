import sys
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.utils.dates import days_ago
import importlib.util
import os
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
python_callable_module_check_task = import_module("utilities.check_task_state")
python_callable_check_task = getattr(python_callable_module_check_task, "check_most_recent_task_state")

# Path to directory where DAG files are stored
DAG_FOLDER = "/opt/airflow/dags/"
import re

def sanitize_task_id(task_id):
    """Ensure task_id is compatible with Airflow's character requirements."""
    # Replace any characters that are not alphanumeric, dash, dot, or underscore with an underscore
    return re.sub(r'[^a-zA-Z0-9._-]', '_', task_id)
def load_dag_module(dag_file):
    """Load a DAG file as a Python module to access its tasks."""
    module_name = os.path.basename(dag_file).replace(".py", "")
    spec = importlib.util.spec_from_file_location(module_name, dag_file)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module
def load_tasks_from_dag(dag_name, dag, prefix, loaded_dags=set(), task_ids=set()):
    """Recursively load tasks from DAGs, preserving dependencies and updating task IDs for op_kwargs, without shared task_mapping."""
    if dag_name in loaded_dags:
        #print(f"Skipping already loaded DAG: {dag_name}")
        #return []  # Prevent reloading to avoid cyclic dependencies
        pass

    # Local task_mapping for this specific call
    task_mapping = {}

    # Mark DAG as loaded to avoid reloading
    loaded_dags.add(dag_name)
    #print(f"Loading DAG: {dag_name}")

    # Load the DAG file and retrieve the DAG object
    dag_file = os.path.join(DAG_FOLDER, f"{dag_name}.py")
    module = load_dag_module(dag_file)
    referenced_dag = next((d for d in module.__dict__.values() if isinstance(d, DAG) and d.dag_id == dag_name), None)
    #print (f"this is the dag {referenced_dag}")

    if not referenced_dag:
        raise AttributeError(f"DAG '{dag_name}' not found in the module '{module.__name__}'. Ensure the dag_id matches.")

    success_dag_name = None
    failure_dag_name = None

    # Process each task in the referenced DAG
    for task in referenced_dag.tasks:
        unique_task_id = sanitize_task_id(f"{prefix}_{task.task_id}")
        
        # Check for duplicates and skip if already registered
        if unique_task_id in task_ids:
            #print(f"Skipping duplicate task: {unique_task_id}")
            continue

        # Clone the task based on its type
        if isinstance(task, BranchPythonOperator):
            updated_op_kwargs = task.op_kwargs.copy()
            updated_op_kwargs['task_id'] = unique_task_id
            updated_op_kwargs['dag_id'] = dag.dag_id

            cloned_task = BranchPythonOperator(
                task_id=unique_task_id,
                python_callable=task.python_callable,
                op_kwargs=updated_op_kwargs,
                dag=dag
            )
        elif isinstance(task, PythonOperator):
            cloned_task = PythonOperator(
                task_id=unique_task_id,
                python_callable=task.python_callable,
                op_kwargs=task.op_kwargs,
                dag=dag
            )

        # Track the cloned task in task_mapping with its unique ID
        task_mapping[unique_task_id] = cloned_task
        task_ids.add(unique_task_id)
        #print(f"Registered task: {unique_task_id} in DAG: {dag.dag_id}")

        # Identify success and failure paths by capturing target DAG names
        if isinstance(task, TriggerDagRunOperator):
            if task.task_id.endswith("on_success"):
                success_dag_name = task.trigger_dag_id
            elif task.task_id.endswith("on_failure"):
                failure_dag_name = task.trigger_dag_id

    # Set dependencies within local task_mapping
    for task in referenced_dag.tasks:
        task_unique_id = f"{prefix}_{task.task_id}"
        for downstream_task in task.downstream_list:
            downstream_unique_id = f"{prefix}_{downstream_task.task_id}"
            #print (f"-----------------{task_mapping}")
            
            # Prevent self-dependency and skip identical mappings
            if (
                task_unique_id == downstream_unique_id or
                (task_unique_id in task_mapping and downstream_unique_id in task_mapping and 
                 task_mapping[task_unique_id] == task_mapping[downstream_unique_id])
            ):
                #print(f"Skipping self-referencing dependency for task: {task.task_id}")
                continue

            # Set dependency if both tasks are in task_mapping
            if task_unique_id in task_mapping and downstream_unique_id in task_mapping:
                #print(f"Setting dependency: {task_mapping[task_unique_id].task_id} >> {task_mapping[downstream_unique_id].task_id}")
                task_mapping[task_unique_id] >> task_mapping[downstream_unique_id]

    # Access the second task in task_mapping (the BranchPythonOperator) directly
    branch_task = None
    task_keys = list(task_mapping.keys())
    if len(task_keys) > 1:
        branch_task = task_mapping[task_keys[1]]  # Get the second element, assumed to be BranchPythonOperator

    # Set dependencies using the identified branch_task for success and failure paths
    if branch_task and success_dag_name:
        success_tasks = load_tasks_from_dag(success_dag_name, dag, f"{prefix}_{success_dag_name}", loaded_dags, task_ids)
        if success_tasks and isinstance(success_tasks[0], PythonOperator):
            #print(f"Setting dependency: {branch_task} >> {success_tasks[0]}")
            branch_task >> success_tasks[0]  # Use branch_task directly here for the success path'
            branch_task.op_kwargs['success_path'] = success_tasks[0].task_id
            

    if branch_task and failure_dag_name:
        failure_tasks = load_tasks_from_dag(failure_dag_name, dag, f"{prefix}_{failure_dag_name}", loaded_dags, task_ids)
        if failure_tasks and isinstance(failure_tasks[0], PythonOperator):
            #print(f"Setting dependency: {branch_task} >> {failure_tasks[0]}")
            branch_task >> failure_tasks[0]  # Use branch_task directly here for the failure path
            branch_task.op_kwargs['fail_path'] = failure_tasks[0].task_id

    #print(f"{dag_name} tasks loaded into main DAG with hierarchical structure.")
    return list(task_mapping.values())
def load_tasks_from_dag5(dag_name, dag, prefix, loaded_dags=set(), task_mapping={}, task_ids=set()):
    """Recursively load tasks from DAGs, preserving dependencies and updating task IDs for op_kwargs."""
    if dag_name in loaded_dags:
        print(f"Skipping already loaded DAG: {dag_name}")
        return []  # Prevent reloading to avoid cyclic dependencies

    # Mark DAG as loaded to avoid reloading
    loaded_dags.add(dag_name)
    print(f"Loading DAG: {dag_name}")

    # Load the DAG file and retrieve the DAG object
    dag_file = os.path.join(DAG_FOLDER, f"{dag_name}.py")
    module = load_dag_module(dag_file)
    referenced_dag = next((d for d in module.__dict__.values() if isinstance(d, DAG) and d.dag_id == dag_name), None)

    if not referenced_dag:
        raise AttributeError(f"DAG '{dag_name}' not found in the module '{module.__name__}'. Ensure the dag_id matches.")

    success_dag_name = None
    failure_dag_name = None

    # Process each task in the referenced DAG
    for task in referenced_dag.tasks:
        unique_task_id = sanitize_task_id(f"{prefix}_{task.task_id}")
        
        # Check for duplicates and skip if already registered
        if unique_task_id in task_ids:
            print(f"Skipping duplicate task: {unique_task_id}")
            continue

        # Clone the task based on its type
        if isinstance(task, BranchPythonOperator):
            # Update `op_kwargs` to refer to the unique task_id and main DAG's dag_id
            updated_op_kwargs = task.op_kwargs.copy()
            updated_op_kwargs['task_id'] = unique_task_id
            updated_op_kwargs['dag_id'] = dag.dag_id

            cloned_task = BranchPythonOperator(
                task_id=unique_task_id,
                python_callable=task.python_callable,
                op_kwargs=updated_op_kwargs,
                dag=dag
            )
        elif isinstance(task, PythonOperator):
            cloned_task = PythonOperator(
                task_id=unique_task_id,
                python_callable=task.python_callable,
                op_kwargs=task.op_kwargs,
                dag=dag
            )

        # Track the cloned task in task_mapping with its unique ID
        task_mapping[task.task_id] = cloned_task
        task_ids.add(unique_task_id)
        print(f"Registered task: {unique_task_id} in DAG: {dag.dag_id}")

        # Identify success and failure paths by capturing target DAG names
        if isinstance(task, TriggerDagRunOperator):
            if task.task_id.endswith("on_success"):
                success_dag_name = task.trigger_dag_id
            elif task.task_id.endswith("on_failure"):
                failure_dag_name = task.trigger_dag_id

    # Set dependencies using unique_task_ids for each downstream task
    for task in referenced_dag.tasks:
        task_unique_id = f"{prefix}_{task.task_id}"
        for downstream_task in task.downstream_list:
            downstream_unique_id = f"{prefix}_{downstream_task.task_id}"

            # Ensure no self-dependency and that both tasks are in task_mapping
            if task_unique_id == downstream_unique_id or task_unique_id not in task_mapping or downstream_unique_id not in task_mapping or task_mapping[task_unique_id] == task_mapping[downstream_unique_id]:
                print(f"Skipping self-dependency for task: {task_unique_id}")
                continue

            if task_unique_id in task_mapping and downstream_unique_id in task_mapping:
                print(f"Setting dependency: {task_mapping[task_unique_id].task_id} >> {task_mapping[downstream_unique_id].task_id}")
                task_mapping[task_unique_id] >> task_mapping[downstream_unique_id]

    # Recursively load and link tasks for success and failure paths directly, instead of triggering separate DAG runs
    if success_dag_name and success_dag_name not in loaded_dags:
        success_tasks = load_tasks_from_dag(success_dag_name, dag, f"{prefix}_{success_dag_name}", loaded_dags, task_mapping, task_ids)
        if success_tasks:
            # Link the last task of the current DAG's success path to the first task of the next DAG
            task_mapping[unique_task_id] >> success_tasks[0]

    if failure_dag_name and failure_dag_name not in loaded_dags:
        failure_tasks = load_tasks_from_dag(failure_dag_name, dag, f"{prefix}_{failure_dag_name}", loaded_dags, task_mapping, task_ids)
        if failure_tasks:
            # Link the last task of the current DAG's failure path to the first task of the next DAG
            task_mapping[unique_task_id] >> failure_tasks[0]

    print(f"{dag_name} tasks loaded into main DAG.")
    return list(task_mapping.values())

def load_tasks_from_dag3(dag_name, dag, prefix, loaded_dags=set(), task_mapping={}, task_ids=set()):
    """Recursively load tasks from DAGs, preserving dependencies and updating task IDs for op_kwargs."""
    if dag_name in loaded_dags:
        print(f"Skipping already loaded DAG: {dag_name}")
        return []  # Prevent reloading to avoid cyclic dependencies

    # Mark DAG as loaded to avoid reloading
    loaded_dags.add(dag_name)
    print(f"Loading DAG: {dag_name}")

    # Load the DAG file and retrieve the DAG object
    dag_file = os.path.join(DAG_FOLDER, f"{dag_name}.py")
    module = load_dag_module(dag_file)
    referenced_dag = next((d for d in module.__dict__.values() if isinstance(d, DAG) and d.dag_id == dag_name), None)

    if not referenced_dag:
        raise AttributeError(f"DAG '{dag_name}' not found in the module '{module.__name__}'. Ensure the dag_id matches.")

    success_dag_name = None
    failure_dag_name = None

    # Process each task in the referenced DAG
    for task in referenced_dag.tasks:
        # Create a unique task ID using prefix to avoid conflicts
        unique_task_id = sanitize_task_id(f"{prefix}_{task.task_id}")

        # Skip adding the task if it already exists in task_ids
        if unique_task_id in task_ids:
            print(f"Skipping duplicate task: {unique_task_id}")
            continue

        # Clone the task and assign it a unique task ID
        if isinstance(task, BranchPythonOperator):
            updated_op_kwargs = task.op_kwargs.copy()
            updated_op_kwargs['task_id'] = unique_task_id
            updated_op_kwargs['dag_id'] = dag.dag_id

            cloned_task = task.__class__(
                task_id=unique_task_id,
                python_callable=task.python_callable,
                op_kwargs=updated_op_kwargs,
                dag=dag
            )
        elif isinstance(task, PythonOperator):
            cloned_task = task.__class__(
                task_id=unique_task_id,
                python_callable=task.python_callable,
                op_kwargs=task.op_kwargs,
                dag=dag
            )

        # Add the cloned task to the global task_mapping
        task_mapping[unique_task_id] = cloned_task
        task_ids.add(unique_task_id)
        print(f"Registered task: {unique_task_id} in DAG: {dag.dag_id}")

        # Identify TriggerDagRunOperator and capture target DAGs
        if isinstance(task, TriggerDagRunOperator):
            if task.task_id.endswith("on_success"):
                success_dag_name = task.trigger_dag_id
            elif task.task_id.endswith("on_failure"):
                failure_dag_name = task.trigger_dag_id

    # Set dependencies using unique_task_ids
    for task in referenced_dag.tasks:
        task_unique_id = f"{prefix}_{task.task_id}"
        for downstream_task in task.downstream_list:
            downstream_unique_id = f"{prefix}_{downstream_task.task_id}"

            # Ensure no self-dependency and that both tasks are in task_mapping
            if task_unique_id == downstream_unique_id:
                print(f"Skipping self-dependency for task: {task_unique_id}")
                continue

            if task_unique_id in task_mapping and downstream_unique_id in task_mapping:
                print(f"Setting dependency: {task_mapping[task_unique_id].task_id} >> {task_mapping[downstream_unique_id].task_id}")
                task_mapping[task_unique_id] >> task_mapping[downstream_unique_id]

    # Recursively load and link tasks for success and failure paths
    if success_dag_name and success_dag_name not in loaded_dags:
        success_tasks = load_tasks_from_dag(success_dag_name, dag, f"{prefix}_{success_dag_name}", loaded_dags, task_mapping, task_ids)
        if success_tasks:
            print(f"Linking success path from {task_mapping[next(iter(task_mapping))].task_id} to {success_tasks[0].task_id}")
            task_mapping[next(iter(task_mapping))] >> success_tasks[0]

    if failure_dag_name and failure_dag_name not in loaded_dags:
        failure_tasks = load_tasks_from_dag(failure_dag_name, dag, f"{prefix}_{failure_dag_name}", loaded_dags, task_mapping, task_ids)
        if failure_tasks:
            print(f"Linking failure path from {task_mapping[next(iter(task_mapping))].task_id} to {failure_tasks[0].task_id}")
            task_mapping[next(iter(task_mapping))] >> failure_tasks[0]

    return list(task_mapping.values())

def load_tasks_from_dag7(dag_name, dag, prefix, loaded_dags=set(), task_ids=set()):
    """Recursively load tasks from DAGs, preserving dependencies and updating task IDs for op_kwargs."""
    if dag_name in loaded_dags:
        print(f"Skipping already loaded DAG: {dag_name}")
        return []  # Prevent reloading to avoid cyclic dependencies

    # Mark DAG as loaded to avoid reloading
    loaded_dags.add(dag_name)
    print(f"Loading DAG: {dag_name}")

    # Load the DAG file and retrieve the DAG object
    dag_file = os.path.join(DAG_FOLDER, f"{dag_name}.py")
    module = load_dag_module(dag_file)
    referenced_dag = next((d for d in module.__dict__.values() if isinstance(d, DAG) and d.dag_id == dag_name), None)

    if not referenced_dag:
        raise AttributeError(f"DAG '{dag_name}' not found in the module '{module.__name__}'. Ensure the dag_id matches.")

    task_mapping = {}
    success_dag_name = None
    failure_dag_name = None

    # Process each task in the referenced DAG
    for task in referenced_dag.tasks:
        unique_task_id = sanitize_task_id(f"{prefix}_{task.task_id}")
        
        # Check for duplicates and skip if already registered
        if unique_task_id in task_ids:
            print(f"Skipping duplicate task: {unique_task_id}")
            continue

        # Clone the task based on its type
        if isinstance(task, BranchPythonOperator):
            updated_op_kwargs = task.op_kwargs.copy()
            updated_op_kwargs['task_id'] = unique_task_id
            updated_op_kwargs['dag_id'] = dag.dag_id

            cloned_task = task.__class__(
                task_id=unique_task_id,
                python_callable=task.python_callable,
                op_kwargs=updated_op_kwargs,
                dag=dag
            )
        elif isinstance(task, PythonOperator):
            cloned_task = task.__class__(
                task_id=unique_task_id,
                python_callable=task.python_callable,
                op_kwargs=task.op_kwargs,
                dag=dag
            )
        
        # Track the task in the main DAG
        task_mapping[task.task_id] = cloned_task
        task_ids.add(unique_task_id)
        print(f"Registered task: {unique_task_id} in DAG: {dag.dag_id}")

        # Identify TriggerDagRunOperator and capture target DAGs
        if isinstance(task, TriggerDagRunOperator):
            if task.task_id.endswith("on_success"):
                success_dag_name = task.trigger_dag_id
            elif task.task_id.endswith("on_failure"):
                failure_dag_name = task.trigger_dag_id

    # Set dependencies based on the original DAG structure
    # Set dependencies based on the original DAG structure
    for task in referenced_dag.tasks:
        task_unique_id = f"{prefix}_{task.task_id}"
        for downstream_task in task.downstream_list:
            downstream_unique_id = f"{prefix}_{downstream_task.task_id}"
            # Prevent a task from being set as a dependency on itself
            if task_unique_id == downstream_unique_id or (task_unique_id in task_mapping and downstream_unique_id in task_mapping and task_mapping[task_unique_id] == task_mapping[downstream_unique_id]):
                print(f"Skipping self-referencing dependency for task: {task.task_id}")
                continue

            # Ensure both tasks are in the task mapping before setting dependencies
            if task_unique_id in task_mapping and downstream_unique_id in task_mapping:
                pass
                
            print(f"Setting dependency: {task_mapping[task_unique_id].task_id} >> {task_mapping[downstream_unique_id].task_id}")
            task_mapping[task_unique_id] >> task_mapping[downstream_unique_id]
        ## What if the task does not have downstrram...it should mention task in that case


    # Recursively load and link tasks for success and failure paths
    if success_dag_name and success_dag_name not in loaded_dags:
        success_tasks = load_tasks_from_dag(success_dag_name, dag, f"{prefix}_{success_dag_name}", loaded_dags, task_ids)
        if success_tasks:
            print(f"Linking success path from {task_mapping[next(iter(task_mapping))].task_id} to {success_tasks[0].task_id}")
            ## get the branch task, the code below only gets the first one which could be very well the phython operator
            task_mapping[next(iter(task_mapping))] >> success_tasks[0]

    if failure_dag_name and failure_dag_name not in loaded_dags:
        failure_tasks = load_tasks_from_dag(failure_dag_name, dag, f"{prefix}_{failure_dag_name}", loaded_dags, task_ids)
        if failure_tasks:
            print(f"Linking failure path from {task_mapping[next(iter(task_mapping))].task_id} to {failure_tasks[0].task_id}")
            task_mapping[next(iter(task_mapping))] >> failure_tasks[0]
    print (f"{dag_name} -------->> {list(task_mapping.values())}")
    return list(task_mapping.values())

def load_tasks_from_dag1(dag_name, dag, prefix, loaded_dags=set(),task_ids=set()):

    """Recursively load tasks from DAGs, preserving dependencies and updating task IDs for op_kwargs."""
    if dag_name in loaded_dags:
        return []  # Prevent reloading to avoid cyclic dependencies

    # Mark DAG as loaded to avoid reloading
    loaded_dags.add(dag_name)
    
    # Load the DAG file and retrieve the DAG object
    dag_file = os.path.join(DAG_FOLDER, f"{dag_name}.py")
    module = load_dag_module(dag_file)
    #referenced_dag = getattr(module, dag_name)
    referenced_dag = next((d for d in module.__dict__.values() if isinstance(d, DAG) and d.dag_id == dag_name), None)
    
    # Store tasks and a mapping for any success/failure paths
    task_mapping = {}
    success_dag_name = None
    failure_dag_name = None

    # Add all tasks from the DAG to the main DAG, preserving task definitions
    for task in referenced_dag.tasks:
        # Create a unique task ID with a prefix to avoid conflicts
        unique_task_id = f"{prefix}_{task.task_id}"
        print (f"this is the {unique_task_id} and the set is {task_ids}")
        if unique_task_id in task_ids:
            print(f"Skipping duplicate task: {unique_task_id}")
            continue
        
        # Clone the task and add it to the main DAG
        if isinstance(task, BranchPythonOperator):
            # Update `op_kwargs` to reference the new PythonOperator task name and main DAG's dag_id
            print(f"{task} and the value of the dag is {task.task_id} and {dag_name} and {unique_task_id}")
            original_python_task_id = task.op_kwargs.get('task_id')
            updated_task_id = f"{prefix}_{original_python_task_id}"
            print(f"{unique_task_id} and {updated_task_id}")
            updated_op_kwargs = task.op_kwargs.copy()
            updated_op_kwargs['task_id'] = unique_task_id
            updated_op_kwargs['dag_id'] = dag.dag_id  # Set to main DAG's dag_id

            cloned_task = task.__class__(
                task_id=unique_task_id,
                python_callable=task.python_callable,
                op_kwargs=updated_op_kwargs,
                dag=dag
            )
        elif isinstance(task, PythonOperator):
            cloned_task = task.__class__(
                task_id=unique_task_id,
                python_callable=task.python_callable,
                op_kwargs=task.op_kwargs,
                dag=dag
            )

        #dag.add_task(cloned_task)
        task_mapping[task.task_id] = cloned_task
        task_ids.add(unique_task_id)

        # Identify TriggerDagRunOperator and capture target DAGs
        if isinstance(task, TriggerDagRunOperator):
            if task.task_id.endswith("on_success"):
                success_dag_name = task.trigger_dag_id
            elif task.task_id.endswith("on_failure"):
                failure_dag_name = task.trigger_dag_id

    # Set original dependencies within the imported tasks
    for task in referenced_dag.tasks:
        for downstream_task in task.downstream_list:
            task_mapping[task.task_id] >> task_mapping[downstream_task.task_id]

    # Recursively load and link tasks for success and failure paths
    if success_dag_name and success_dag_name not in loaded_dags:
        success_tasks = load_tasks_from_dag(success_dag_name, dag, f"{prefix}_{success_dag_name}", loaded_dags, task_ids)
        if success_tasks:
            task_mapping[next(iter(task_mapping))] >> success_tasks[0]  # Link first task in success path

    if failure_dag_name and failure_dag_name not in loaded_dags:
        failure_tasks = load_tasks_from_dag(failure_dag_name, dag, f"{prefix}_{failure_dag_name}", loaded_dags, task_ids)
        if failure_tasks:
            task_mapping[next(iter(task_mapping))] >> failure_tasks[0]  # Link first task in failure path

    return list(task_mapping.values())

# Main merged DAG definition
with DAG(
    'merged_dag_with_dynamic_dag_id',
    default_args={'start_date': days_ago(1)},
    schedule_interval=None
) as dag:

    # Start by loading tasks from the root DAG (e.g., "workflow_21")
    main_task_sequence = load_tasks_from_dag("workflow_21", dag, "workflow_21", set())
    #print (main_task_sequence)
