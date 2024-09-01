##############################################################################
# Import the necessary modules
# #############################################################################

import warnings
warnings.filterwarnings("ignore")

import pandas as pd
import os
import sqlite3
from sqlite3 import Error

from utils import *

from constants import *
from city_tier_mapping import *
from significant_categorical_level import *

def check_testdatapoints_present_in_result(df_result_dataset, df_test_dataset):
    
    list_rowno_testdatapoints_failed = []
    for idx,test_row in df_test_dataset.iterrows():
        df_filter = df_result_dataset
        for col in test_row.index:
            if not pd.isnull(test_row[col]):
                df_filter = df_filter[ df_filter[col]==test_row[col] ]
            else:
                df_filter = df_filter[ pd.isnull(df_filter[col]) ]
        is_match_found = True if len(df_filter.index)==1 else False
        if not is_match_found:
            list_rowno_testdatapoints_failed += [idx]
    
    assert len(list_rowno_testdatapoints_failed)==0, "Following indices of test datapoints not found: {list_rowno_testdatapoints_failed}"

###############################################################################
# Write test cases for load_data_into_db() function
# ##############################################################################

def test_load_data_into_db():
    """_summary_
    This function checks if the load_data_into_db function is working properly by
    comparing its output with test cases provided in the db in a table named
    'loaded_data_test_case'

    INPUTS
        DB_FILE_NAME : Name of the database file 'utils_output.db'
        DB_PATH : path where the db file should be present
        UNIT_TEST_DB_FILE_NAME: Name of the test database file 'unit_test_cases.db'

    SAMPLE USAGE
        output=test_get_data()

    """
    
    build_dbs()
    load_data_into_db()
    
    utils_output_db_conn = sqlite3.connect(DB_PATH+DB_FILE_NAME)
    df_loaded_data = pd.read_sql("SELECT * FROM loaded_data", utils_output_db_conn)
    
    unit_test_db_conn = sqlite3.connect("unit_test_cases.db")
    df_loaded_data_test_case = pd.read_sql("SELECT * FROM loaded_data_test_case", unit_test_db_conn)
    
    check_testdatapoints_present_in_result(df_loaded_data, df_loaded_data_test_case)
    

###############################################################################
# Write test cases for map_city_tier() function
# ##############################################################################
def test_map_city_tier():
    """_summary_
    This function checks if map_city_tier function is working properly by
    comparing its output with test cases provided in the db in a table named
    'city_tier_mapped_test_case'

    INPUTS
        DB_FILE_NAME : Name of the database file 'utils_output.db'
        DB_PATH : path where the db file should be present
        UNIT_TEST_DB_FILE_NAME: Name of the test database file 'unit_test_cases.db'

    SAMPLE USAGE
        output=test_map_city_tier()

    """
    
    map_city_tier()
    
    utils_output_db_conn = sqlite3.connect(DB_PATH+DB_FILE_NAME)
    df_city_tier_mapped = pd.read_sql("SELECT * FROM city_tier_mapped", utils_output_db_conn)
    
    unit_test_db_conn = sqlite3.connect("unit_test_cases.db")
    df_city_tier_mapped_test_case = pd.read_sql("SELECT * FROM city_tier_mapped_test_case", unit_test_db_conn)
    
    check_testdatapoints_present_in_result(df_city_tier_mapped, df_city_tier_mapped_test_case)
    

###############################################################################
# Write test cases for map_categorical_vars() function
# ##############################################################################    
def test_map_categorical_vars():
    """_summary_
    This function checks if map_cat_vars function is working properly by
    comparing its output with test cases provided in the db in a table named
    'categorical_variables_mapped_test_case'

    INPUTS
        DB_FILE_NAME : Name of the database file 'utils_output.db'
        DB_PATH : path where the db file should be present
        UNIT_TEST_DB_FILE_NAME: Name of the test database file 'unit_test_cases.db'
    
    SAMPLE USAGE
        output=test_map_cat_vars()

    """    
    
    map_categorical_vars()
    
    utils_output_db_conn = sqlite3.connect(DB_PATH+DB_FILE_NAME)
    df_categorical_variables_mapped = pd.read_sql("SELECT * FROM categorical_variables_mapped", utils_output_db_conn)
    
    unit_test_db_conn = sqlite3.connect("unit_test_cases.db")
    df_categorical_variables_mapped_test_case = pd.read_sql("SELECT * FROM categorical_variables_mapped_test_case", unit_test_db_conn)
    
    check_testdatapoints_present_in_result(df_categorical_variables_mapped, df_categorical_variables_mapped_test_case)
    

###############################################################################
# Write test cases for interactions_mapping() function
# ##############################################################################    
def test_interactions_mapping():
    """_summary_
    This function checks if test_column_mapping function is working properly by
    comparing its output with test cases provided in the db in a table named
    'interactions_mapped_test_case'

    INPUTS
        DB_FILE_NAME : Name of the database file 'utils_output.db'
        DB_PATH : path where the db file should be present
        UNIT_TEST_DB_FILE_NAME: Name of the test database file 'unit_test_cases.db'

    SAMPLE USAGE
        output=test_column_mapping()

    """ 
    
    interactions_mapping()
    
    utils_output_db_conn = sqlite3.connect(DB_PATH+DB_FILE_NAME)
    df_model_input = pd.read_sql("SELECT * FROM model_input", utils_output_db_conn)
    
    unit_test_db_conn = sqlite3.connect("unit_test_cases.db")
    df_interactions_mapped_test_case = pd.read_sql("SELECT * FROM interactions_mapped_test_case", unit_test_db_conn)
    
    check_testdatapoints_present_in_result(df_model_input, df_interactions_mapped_test_case)
