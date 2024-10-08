'''
filename: utils.py
functions: encode_features, load_model
creator: shashank.gupta
version: 1
'''

###############################################################################
# Import necessary modules
# ##############################################################################

import mlflow
import mlflow.sklearn
import pandas as pd

import sqlite3

import os
import logging
import time

from datetime import datetime

from constants import *

###############################################################################
# Define the function to train the model
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
        **NOTE : You can modify the encode_featues function used in heart disease's inference
        pipeline for this.

    OUTPUT
        1. Save the encoded features in a table - features

    SAMPLE USAGE
        encode_features()
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
    
    ##Saving target and encoded features in separate tables
    encoded_df.to_sql("features", conn, if_exists='replace')
    conn.close()
    

###############################################################################
# Define the function to load the model from mlflow model registry
# ##############################################################################

def get_models_prediction():
    '''
    This function loads the model which is in production from mlflow registry and 
    uses it to do prediction on the input dataset. Please note this function will the load
    the latest version of the model present in the production stage. 

    INPUTS
        db_file_name : Name of the database file
        db_path : path where the db file should be
        model from mlflow model registry
        model name: name of the model to be loaded
        stage: stage from which the model needs to be loaded i.e. production


    OUTPUT
        Store the predicted values along with input data into a table

    SAMPLE USAGE
        load_model()
    '''
    
    conn = sqlite3.connect(DB_PATH+DB_FILE_NAME)
    features_df = pd.read_sql("SELECT * FROM features", conn, index_col="index")
    
    mlflow.set_tracking_uri(TRACKING_URI)
    model_uri = f"models:/{MODEL_NAME}/{STAGE}"
    model = mlflow.sklearn.load_model(model_uri)
    predictions_df = pd.DataFrame(model.predict(features_df), columns=["predictions"])
    
    predictions_df.to_sql("predictions", conn, if_exists='replace')
    conn.close()
    

###############################################################################
# Define the function to check the distribution of output column
# ##############################################################################
def prediction_ratio_check():
    '''
    This function calculates the % of 1 and 0 predicted by the model and  
    and writes it to a file named 'prediction_distribution.txt'.This file 
    should be created in the ~/airflow/dags/Lead_scoring_inference_pipeline 
    folder. 
    This helps us to monitor if there is any drift observed in the predictions 
    from our model at an overall level. This would determine our decision on 
    when to retrain our model.
    

    INPUTS
        db_file_name : Name of the database file
        db_path : path where the db file should be

    OUTPUT
        Write the output of the monitoring check in prediction_distribution.txt with 
        timestamp.

    SAMPLE USAGE
        prediction_col_check()
    '''
    
    conn = sqlite3.connect(DB_PATH+DB_FILE_NAME)
    predictions_df = pd.read_sql("SELECT * FROM predictions", conn, index_col="index")
    conn.close()
    
    percentage_of_positive_predicted = float(round((predictions_df.sum()/predictions_df.count()) * 100, 2))
    percentage_of_negative_predicted = 100 - percentage_of_positive_predicted
    
    with open(PREDICTION_DISTRIBUTION_FILE_PATH, "a") as f:
        f.write(f"{time.time()} | Percentage of positive predictions: {percentage_of_positive_predicted} % | Percentage of negative predictions: {percentage_of_negative_predicted} %\n")
    
    
###############################################################################
# Define the function to check the columns of input features
# ##############################################################################
   

def input_features_check():
    '''
    This function checks whether all the input columns are present in our new
    data. This ensures the prediction pipeline doesn't break because of change in
    columns in input data.

    INPUTS
        db_file_name : Name of the database file
        db_path : path where the db file should be
        ONE_HOT_ENCODED_FEATURES: List of all the features which need to be present
        in our input data.

    OUTPUT
        It writes the output in a log file based on whether all the columns are present
        or not.
        1. If all the input columns are present then it logs - 'All the models input are present'
        2. Else it logs 'Some of the models inputs are missing'

    SAMPLE USAGE
        input_col_check()
    '''
    
    conn = sqlite3.connect(DB_PATH+DB_FILE_NAME)
    features_df = pd.read_sql("SELECT * FROM features", conn, index_col="index")
    conn.close()
    
    features_missing = set(ONE_HOT_ENCODED_FEATURES) - set(features_df.columns)
    with open(INPUT_FEATURES_CHECK_FILE_PATH, "a") as f:
        if features_missing:
            f.write(f"{time.time()} | Some of the models inputs are missing\n")
        else:
            f.write(f"{time.time()} | All the models input are present\n")
        
    