DB_PATH = "/home/Assignment/03_inference_pipeline/scripts/"
DB_FILE_NAME = "utils_output.db"

DB_FILE_MLFLOW = "sqlite:////home/Lead_scoring_mlflow_production.db"

PREDICTION_DISTRIBUTION_FILE_PATH = "/home/Assignment/03_inference_pipeline/scripts/prediction_distribution.txt"
INPUT_FEATURES_CHECK_FILE_PATH = "/home/Assignment/03_inference_pipeline/scripts/input_features_check.log"

TRACKING_URI = "http://0.0.0.0:6006"

# experiment, model name and stage to load the model from mlflow model registry
MODEL_NAME = "Lead_scoring_model"
STAGE = "Production"
EXPERIMENT = "Lead_scoring_mlflow_production"

# list of the features that needs to be there in the final encoded dataframe
ONE_HOT_ENCODED_FEATURES = [ "city_tier", "first_platform_c_", "first_utm_medium_c_", 
                            "first_utm_source_c_", 'total_leads_droppped', 'referred_lead',
                            'assistance_interaction', 'career_interaction',
                            'payment_interaction', 'social_interaction', 'syllabus_interaction' ]

# list of features that need to be one-hot encoded
FEATURES_TO_ENCODE = [ "first_platform_c", "first_utm_medium_c", "first_utm_source_c" ]
