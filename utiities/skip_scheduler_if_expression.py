from datetime import datetime
from airflow.models import Variable

def skip_if_not_nth_cycle(**kwargs):
    """
    This function checks if the current cycle (weeks or months) matches the freqInterval.
    If it's not part of the N-week or N-month cycle, the DAG run will be skipped.
    """
    # Get the current DAG run info
    dag_run = kwargs.get('dag_run')

    # Bypass skip logic if the DAG is triggered manually or by TriggerDagRunOperator
    if dag_run and dag_run.external_trigger:
        print("DAG was triggered manually or by TriggerDagRunOperator. Bypassing cycle check.")
        return 'python_task'

    # Extract relevant parameters from op_kwargs
    cycle_type = kwargs['op_kwargs'].get('cycleType', 'week')  # Either 'week' or 'month'
    freq_interval = kwargs['op_kwargs'].get('freqInterval', 1)  # Default to 1 if not provided
    start_date_str = kwargs['op_kwargs'].get('start_date', None)  # Start date string in 'YYYY-MM-DD' format
    end_date_str = kwargs['op_kwargs'].get('end_date', None)  # End date string in 'YYYY-MM-DD' format

    # Convert start_date and end_date to datetime objects
    start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
    current_date = datetime.now()

    # Check if the current date is past the end_date (optional check)
    if end_date_str:
        end_date = datetime.strptime(end_date_str, '%Y-%m-%d')
        if current_date > end_date:
            print("Skipping run. Current date is past the end date.")
            return 'skip_task'

    # Determine the type of cycle (weekly or monthly)
    if cycle_type == 'week':
        # Calculate the number of weeks since the start_date
        delta_weeks = (current_date - start_date).days // 7
        print(f"Weeks since start: {delta_weeks}, FreqInterval: {freq_interval}")
        # If the current week is not a multiple of freq_interval, skip the task
        if delta_weeks % freq_interval != 0:
            print(f"Skipping run. Current week is not part of the {freq_interval}-week cycle.")
            return 'skip_task'
    
    elif cycle_type == 'month':
        # Calculate the number of months since the start_date
        delta_months = (current_date.year - start_date.year) * 12 + (current_date.month - start_date.month)
        print(f"Months since start: {delta_months}, FreqInterval: {freq_interval}")
        # If the current month is not a multiple of freq_interval, skip the task
        if delta_months % freq_interval != 0:
            print(f"Skipping run. Current month is not part of the {freq_interval}-month cycle.")
            return 'skip_task'

    # If the current cycle matches the N-week or N-month cycle, continue with the actual task
    return 'python_task'
