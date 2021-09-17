WITH prediction_models_prediction AS (

    SELECT * FROM {{ ref('prediction_models_prediction') }}

), ai_blocks_trained AS (

    SELECT * FROM {{ ref('ai_blocks_trained') }}

)

SELECT 
    prediction_models_prediction.account_id,
    prediction_models_prediction.classifier_id,
    date_prediction_made,
    performance
FROM 
    prediction_models_prediction
INNER JOIN 
    ai_blocks_trained ON ai_blocks_trained.classifier_id=prediction_models_prediction.classifier_id
