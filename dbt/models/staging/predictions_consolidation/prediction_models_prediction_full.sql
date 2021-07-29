WITH prediction_models_prediction AS (

    SELECT
       *
    FROM 
       {{ref('prediction_models_prediction')}}

), datasets_dataset AS (

    SELECT
       user_id,
       account_id
    FROM 
       {{ref('datasets_dataset')}}
    GROUP BY
        user_id,
        account_id

)

SELECT 
    prediction_models_prediction.prediction_id,					
    prediction_models_prediction.classifier_id,			    
    prediction_models_prediction.date_prediction_made,		
    prediction_models_prediction.old_user_id,
    CASE 
        WHEN prediction_models_prediction.user_id IS NULL THEN datasets_dataset.user_id
        ELSE prediction_models_prediction.user_id
    END                                                                                                   AS user_id,
    prediction_models_prediction.account_id,		
    prediction_models_prediction.date_prediction_updated,
    prediction_models_prediction._airbyte_emitted_at,	
    prediction_models_prediction._airbyte_production_prediction_models_prediction_hashid
FROM   
    prediction_models_prediction
LEFT JOIN 
    datasets_dataset
    ON datasets_dataset.account_id=prediction_models_prediction.account_id