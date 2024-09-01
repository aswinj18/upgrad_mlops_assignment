'''
filename: utils.py
functions: encode_features, get_train_model
creator: shashank.gupta
version: 1
'''

###############################################################################
# Import necessary modules
# ##############################################################################

import pandas as pd
import numpy as np

import sqlite3
from sqlite3 import Error

import mlflow
import mlflow.sklearn
from pycaret.classification import *

from sklearn.model_selection import train_test_split
from sklearn.metrics import roc_auc_score
import lightgbm as lgb
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score

from constants import *


###############################################################################
# Define the function to encode features
# ##############################################################################

def encode_features():
    '''
    This function one hot encodes the categorical features present in our  
    training dataset. This encoding is needed for feeding categorical data 
    to many scikit-learn models.

    INPUTS
        db_file_name : Name of the database file 
        db_path : path where the db file should be
        ONE_HOT_ENCODED_FEATURES : list of the features that needs to be there in the final encoded dataframe
        FEATURES_TO_ENCODE: list of features  from cleaned data that need to be one-hot encoded
       

    OUTPUT
        1. Save the encoded features in a table - features
        2. Save the target variable in a separate table - target


    SAMPLE USAGE
        encode_features()
        
    **NOTE : You can modify the encode_featues function used in heart disease's inference
        pipeline from the pre-requisite module for this.
    '''
    
    conn = sqlite3.connect(DB_PATH+DB_FILE_NAME)
    preprocessed_df = pd.read_sql("SELECT * FROM model_input", conn)
    
    ## Encoding Features
    
    # Implement these steps to prevent dimension mismatch during inference
    encoded_df = pd.DataFrame(columns= ONE_HOT_ENCODED_FEATURES) # from constants.py
    placeholder_df = pd.DataFrame()
    
    # One-Hot Encoding using get_dummies for the specified categorical features
    for feature in FEATURES_TO_ENCODE:
        if(feature in preprocessed_df.columns):
            encoded = pd.get_dummies(preprocessed_df[feature])
            encoded = encoded.add_prefix(feature + '_')
            placeholder_df = pd.concat([placeholder_df, encoded], axis=1)
        else:
            print('Feature not found')
            return
    
    # Implement these steps to prevent dimension mismatch during inference
    for feature in encoded_df.columns:
        if feature in preprocessed_df.columns:
            encoded_df[feature] = preprocessed_df[feature]
        if feature in placeholder_df.columns:
            encoded_df[feature] = placeholder_df[feature]
            
    # fill all null values
    encoded_df.fillna(0, inplace=True)
    
    ##Separate dataframe for target variable
    target_df = preprocessed_df[TARGET]
    
    ##Saving target and encoded features in separate tables
    encoded_df.to_sql("features", conn, if_exists='replace')
    target_df.to_sql("target", conn, if_exists='replace')
    conn.close()


###############################################################################
# Define the function to train the model
# ##############################################################################

def get_trained_model():
    '''
    This function setups mlflow experiment to track the run of the training pipeline. It 
    also trains the model based on the features created in the previous function and 
    logs the train model into mlflow model registry for prediction. The input dataset is split
    into train and test data and the auc score calculated on the test data and
    recorded as a metric in mlflow run.   

    INPUTS
        db_file_name : Name of the database file
        db_path : path where the db file should be


    OUTPUT
        Tracks the run in experiment named 'Lead_Scoring_Training_Pipeline'
        Logs the trained model into mlflow model registry with name 'LightGBM'
        Logs the metrics and parameters into mlflow run
        Calculate auc from the test data and log into mlflow run  

    SAMPLE USAGE
        get_trained_model()
    '''
    
    mlflow.set_tracking_uri(TRACKING_URI)
    is_experiment_present = mlflow.get_experiment_by_name(EXPERIMENT)
    if not is_experiment_present:
        ignore = mlflow.create_experiment(EXPERIMENT)
    mlflow.set_experiment(EXPERIMENT)

    conn = sqlite3.connect(DB_PATH+DB_FILE_NAME)
    features_df = pd.read_sql("SELECT * FROM features", conn, index_col="index")
    target_df = pd.read_sql("SELECT * FROM target", conn, index_col="index")
    dataset_df = pd.concat([features_df, target_df], axis=1, join="inner")
    
    pipeline_config = setup(data=dataset_df, target='app_complete_flag', 
        silent=True
    )
    lightgbm_model = create_model("lightgbm", **model_config)
    final_model = finalize_model(lightgbm_model)
    
    mlflow.log_params(final_model.get_params())
    mlflow.log_metrics(pull().to_dict(orient='records')[0])
    mlflow.sklearn.log_model(final_model, "LightGBM")
    
    conn.close()
