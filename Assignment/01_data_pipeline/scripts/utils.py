##############################################################################
# Import necessary modules and files
##############################################################################

import pandas as pd
import os
import sqlite3
from sqlite3 import Error

from constants import *
from mapping.city_tier_mapping import *
from mapping.significant_categorical_level import *

###############################################################################
# Define the function to build database
###############################################################################

def build_dbs():
    '''
    This function checks if the db file with specified name is present 
    in the /Assignment/01_data_pipeline/scripts folder. If it is not present it creates 
    the db file with the given name at the given path. 


    INPUTS
        DB_FILE_NAME : Name of the database file 'utils_output.db'
        DB_PATH : path where the db file should exist  


    OUTPUT
    The function returns the following under the given conditions:
        1. If the file exists at the specified path
                prints 'DB Already Exists' and returns 'DB Exists'

        2. If the db file is not present at the specified loction
                prints 'Creating Database' and creates the sqlite db 
                file at the specified path with the specified name and 
                once the db file is created prints 'New DB Created' and 
                returns 'DB created'


    SAMPLE USAGE
        build_dbs()
    '''
    
    db_filepath = DB_PATH+DB_FILE_NAME
    if os.path.exists(db_filepath):
        print("DB Already Exists")
        ret = "DB Exists"
    else:
        print("Creating Database")
        conn = sqlite3.connect(db_filepath)
        conn.close()
        print("New DB Created")
        ret = "DB created"
        
    return ret


###############################################################################
# Define function to load the csv file to the database
###############################################################################

def load_data_into_db():
    '''
    Thie function loads the data present in data directory into the db
    which was created previously.
    It also replaces any null values present in 'toal_leads_dropped' and
    'referred_lead' columns with 0.


    INPUTS
        DB_FILE_NAME : Name of the database file
        DB_PATH : path where the db file should be
        DATA_DIRECTORY : path of the directory where 'leadscoring.csv' 
                        file is present
        

    OUTPUT
        Saves the processed dataframe in the db in a table named 'loaded_data'.
        If the table with the same name already exsists then the function 
        replaces it.


    SAMPLE USAGE
        load_data_into_db()
    '''
    
    if IS_INDEX_IN_RAW_DATA:
        df_lead_scoring = pd.read_csv(DATA_DIRECTORY, index_col=[0])
    else:
        df_lead_scoring = pd.read_csv(DATA_DIRECTORY)
        
    df_lead_scoring['total_leads_droppped'] = df_lead_scoring['total_leads_droppped'].fillna(0)
    df_lead_scoring['referred_lead'] = df_lead_scoring['referred_lead'].fillna(0)
    df_lead_scoring.drop(columns=["created_date"], inplace=True)
    
    conn = sqlite3.connect(DB_PATH+DB_FILE_NAME)
    df_lead_scoring.to_sql("loaded_data", conn, if_exists='replace', index=False)
    conn.close()
    
    
###############################################################################
# Define function to map cities to their respective tiers
###############################################################################
def map_city_tier():
    '''
    This function maps all the cities to their respective tier as per the
    mappings provided in the city_tier_mapping.py file. If a
    particular city's tier isn't mapped(present) in the city_tier_mapping.py 
    file then the function maps that particular city to 3.0 which represents
    tier-3.


    INPUTS
        DB_FILE_NAME : Name of the database file
        DB_PATH : path where the db file should be
        city_tier_mapping : a dictionary that maps the cities to their tier

    
    OUTPUT
        Saves the processed dataframe in the db in a table named
        'city_tier_mapped'. If the table with the same name already 
        exsists then the function replaces it.

    
    SAMPLE USAGE
        map_city_tier()

    '''
    
    conn = sqlite3.connect(DB_PATH+DB_FILE_NAME)
    
    df_lead_scoring = pd.read_sql("SELECT * FROM loaded_data", conn)
    df_lead_scoring["city_tier"] = df_lead_scoring["city_mapped"].map(city_tier_mapping)
    df_lead_scoring["city_tier"] = df_lead_scoring["city_tier"].fillna(3.0)
    df_lead_scoring.drop(['city_mapped'], axis = 1, inplace=True)
    
    df_lead_scoring.to_sql("city_tier_mapped", conn, if_exists='replace', index=False)
    conn.close()

###############################################################################
# Define function to map insignificant categorial variables to "others"
###############################################################################


def map_categorical_vars():
    '''
    This function maps all the insignificant variables present in 'first_platform_c'
    'first_utm_medium_c' and 'first_utm_source_c'. The list of significant variables
    should be stored in a python file in the 'significant_categorical_level.py' 
    so that it can be imported as a variable in utils file.
    

    INPUTS
        DB_FILE_NAME : Name of the database file
        DB_PATH : path where the db file should be present
        list_platform : list of all the significant platform.
        list_medium : list of all the significat medium
        list_source : list of all rhe significant source

        **NOTE : list_platform, list_medium & list_source are all constants and
                 must be stored in 'significant_categorical_level.py'
                 file. The significant levels are calculated by taking top 90
                 percentils of all the levels. For more information refer
                 'data_cleaning.ipynb' notebook.
  

    OUTPUT
        Saves the processed dataframe in the db in a table named
        'categorical_variables_mapped'. If the table with the same name already 
        exsists then the function replaces it.

    
    SAMPLE USAGE
        map_categorical_vars()
    '''
    
    conn = sqlite3.connect(DB_PATH+DB_FILE_NAME)
    df_lead_scoring = pd.read_sql("SELECT * FROM city_tier_mapped", conn)
    
    # for first_platform_c
    new_df = df_lead_scoring[~df_lead_scoring['first_platform_c'].isin(list_platform)] # get rows for levels which are not present in list_platform
    new_df.loc[:,'first_platform_c'] = "others" # replace the value of these levels to others
    old_df = df_lead_scoring[df_lead_scoring['first_platform_c'].isin(list_platform)] # get rows for levels which are present in list_platform
    df_lead_scoring = pd.concat([new_df, old_df]) # concatenate new_df and old_df to get the final dataframe
    
    # for first_utm_medium_c
    new_df = df_lead_scoring[~df_lead_scoring['first_utm_medium_c'].isin(list_medium)] # get rows for levels which are not present in list_medium
    new_df.loc[:,'first_utm_medium_c'] = "others" # replace the value of these levels to others
    old_df = df_lead_scoring[df_lead_scoring['first_utm_medium_c'].isin(list_medium)] # get rows for levels which are present in list_medium
    df_lead_scoring = pd.concat([new_df, old_df]) # concatenate new_df and old_df to get the final dataframe
    
    # for first_utm_source_c
    new_df = df_lead_scoring[~df_lead_scoring['first_utm_source_c'].isin(list_source)] # get rows for levels which are not present in list_source
    new_df.loc[:,'first_utm_source_c'] = "others" # replace the value of these levels to others
    old_df = df_lead_scoring[df_lead_scoring['first_utm_source_c'].isin(list_source)] # get rows for levels which are present in list_source
    df_lead_scoring = pd.concat([new_df, old_df]) # concatenate new_df and old_df to get the final dataframe
    
    df_lead_scoring.to_sql("categorical_variables_mapped", conn, if_exists='replace', index=False)
    conn.close()


##############################################################################
# Define function that maps interaction columns into 4 types of interactions
##############################################################################
def interactions_mapping():
    '''
    This function maps the interaction columns into 4 unique interaction columns
    These mappings are present in 'interaction_mapping.csv' file. 


    INPUTS
        DB_FILE_NAME: Name of the database file
        DB_PATH : path where the db file should be present
        INTERACTION_MAPPING : path to the csv file containing interaction's
                                   mappings
        INDEX_COLUMNS_TRAINING : list of columns to be used as index while pivoting and
                                 unpivoting during training
        INDEX_COLUMNS_INFERENCE: list of columns to be used as index while pivoting and
                                 unpivoting during inference
        NOT_FEATURES: Features which have less significance and needs to be dropped
                                 
        NOTE : Since while inference we will not have 'app_complete_flag' which is
        our label, we will have to exculde it from our features list. It is recommended 
        that you use an if loop and check if 'app_complete_flag' is present in 
        'categorical_variables_mapped' table and if it is present pass a list with 
        'app_complete_flag' column, or else pass a list without 'app_complete_flag'
        column.

    
    OUTPUT
        Saves the processed dataframe in the db in a table named 
        'interactions_mapped'. If the table with the same name already exsists then 
        the function replaces it.
        
        It also drops all the features that are not requried for training model and 
        writes it in a table named 'model_input'

    
    SAMPLE USAGE
        interactions_mapping()
    '''
    
    conn = sqlite3.connect(DB_PATH+DB_FILE_NAME)
    df_lead_scoring = pd.read_sql("SELECT * FROM categorical_variables_mapped", conn)
    
    df_event_mapping = pd.read_csv(INTERACTION_MAPPING, index_col=[0])
    
    df_unpivot = pd.melt(df_lead_scoring, id_vars=['first_platform_c',
       'first_utm_medium_c', 'first_utm_source_c', 'total_leads_droppped', 'city_tier',
       'referred_lead', 'app_complete_flag'], var_name='interaction_type', value_name='interaction_value')
    df_unpivot['interaction_value'] = df_unpivot['interaction_value'].fillna(0)
    df_unpivot = pd.merge(df_unpivot, df_event_mapping, on='interaction_type', how='left')
    df_unpivot = df_unpivot.drop(['interaction_type'], axis=1)
    df_pivot = df_unpivot.pivot_table(
        values='interaction_value', index=['city_tier', 'first_platform_c',
       'first_utm_medium_c', 'first_utm_source_c', 'total_leads_droppped',
       'referred_lead', 'app_complete_flag'], columns='interaction_mapping', aggfunc='sum')
    df_pivot = df_pivot.reset_index()
    
    df_pivot.to_sql("model_input", conn, if_exists='replace', index=False)
    conn.close()