WITH prediction_model_trainingrun AS (

    SELECT 
        *
    FROM 
        {{ref('prediction_model_trainingrun')}}

), prediction_model_classifier AS (
    
    SELECT 
        *
    FROM 
        {{ref('prediction_model_classifier')}}

), onboarded_users AS (

    SELECT
        user_id,
        user_email_address
    FROM
        {{ref('onboarded_users')}}
        
)