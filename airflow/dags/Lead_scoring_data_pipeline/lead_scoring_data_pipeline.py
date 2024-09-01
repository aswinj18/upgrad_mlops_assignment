##############################################################################
# Import necessary modules
# #############################################################################


from airflow import DAG
from airflow.operators.python import PythonOperator

from datetime import datetime, timedelta

from Lead_scoring_data_pipeline.utils import *
from Lead_scoring_data_pipeline.data_validation_checks import *

###############################################################################
# Define default arguments and DAG
# ##############################################################################

default_args = {
    'owner': 'aswinj',
    'start_date': datetime(2023,1,1),
    'retries' : 1, 
    'retry_delay' : timedelta(seconds=5)
}


ML_data_cleaning_dag = DAG(
                dag_id = 'Lead_Scoring_Data_Engineering_Pipeline',
                default_args = default_args,
                description = 'DAG to run data pipeline for lead scoring',
                schedule_interval = '@daily',
                catchup = False
)

###############################################################################
# Create a task for build_dbs() function with task_id 'building_db'
# ##############################################################################
task_build_dbs = PythonOperator(
    task_id = "build_db",
    python_callable = build_dbs,
    dag = ML_data_cleaning_dag
)

###############################################################################
# Create a task for raw_data_schema_check() function with task_id 'checking_raw_data_schema'
# ##############################################################################
task_raw_data_schema_check = PythonOperator(
    task_id = "raw_data_schema_check",
    python_callable = raw_data_schema_check,
    dag = ML_data_cleaning_dag
)

###############################################################################
# Create a task for load_data_into_db() function with task_id 'loading_data'
# #############################################################################
task_load_data_into_db = PythonOperator(
    task_id = "load_data_into_db",
    python_callable = load_data_into_db,
    dag = ML_data_cleaning_dag
)

###############################################################################
# Create a task for map_city_tier() function with task_id 'mapping_city_tier'
# ##############################################################################
task_map_city_tier = PythonOperator(
    task_id = "map_city_tier",
    python_callable = map_city_tier,
    dag = ML_data_cleaning_dag
)

###############################################################################
# Create a task for map_categorical_vars() function with task_id 'mapping_categorical_vars'
# ##############################################################################
task_map_categorical_vars = PythonOperator(
    task_id = "map_categorical_vars",
    python_callable = map_categorical_vars,
    dag = ML_data_cleaning_dag
)

###############################################################################
# Create a task for interactions_mapping() function with task_id 'mapping_interactions'
# ##############################################################################
task_interactions_mapping = PythonOperator(
    task_id = "interactions_mapping",
    python_callable = interactions_mapping,
    dag = ML_data_cleaning_dag
)

###############################################################################
# Create a task for model_input_schema_check() function with task_id 'checking_model_inputs_schema'
# ##############################################################################
task_model_input_schema_check = PythonOperator(
    task_id = "model_input_schema_check",
    python_callable = model_input_schema_check,
    dag = ML_data_cleaning_dag
)

###############################################################################
# Define the relation between the tasks
# ##############################################################################
task_build_dbs >> task_raw_data_schema_check >> task_load_data_into_db >> task_map_city_tier >> task_map_categorical_vars >> task_interactions_mapping >> task_model_input_schema_check

