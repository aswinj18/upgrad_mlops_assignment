DB_PATH = "/home/Assignment/02_training_pipeline/scripts/"
DB_FILE_NAME = "lead_scoring_data_cleaning.db"

DB_FILE_MLFLOW = "sqlite:////home/Lead_scoring_mlflow_production.db"

TRACKING_URI = "http://0.0.0.0:6006"
EXPERIMENT = "Lead_scoring_mlflow_production"


# model config imported from pycaret experimentation
model_config = {
    'boosting_type': 'gbdt',
    'class_weight': None,
    'colsample_bytree': 1.0,
    'importance_type': 'split',
    'learning_rate': 0.05,
    'max_depth': -1,
    'min_child_samples': 6,
    'min_child_weight': 0.001,
    'min_split_gain': 0.1,
    'n_estimators': 140,
    'n_jobs': -1,
    'num_leaves': 40,
    'objective': None,
    'random_state': 3500,
    'reg_alpha': 0.001,
    'reg_lambda': 0.0001,
    'silent': 'warn',
    'subsample': 1.0,
    'subsample_for_bin': 200000,
    'subsample_freq': 0,
    'feature_fraction': 0.8,
    'bagging_freq': 2,
    'bagging_fraction': 0.6
}

# list of the features that needs to be there in the final encoded dataframe
ONE_HOT_ENCODED_FEATURES = [ "city_tier", "first_platform_c_", "first_utm_medium_c_", 
                            "first_utm_source_c_", 'total_leads_droppped', 'referred_lead',
                            'assistance_interaction', 'career_interaction',
                            'payment_interaction', 'social_interaction', 'syllabus_interaction' ]
# list of features that need to be one-hot encoded
FEATURES_TO_ENCODE = [ "first_platform_c", "first_utm_medium_c", "first_utm_source_c" ]

TARGET = "app_complete_flag"
