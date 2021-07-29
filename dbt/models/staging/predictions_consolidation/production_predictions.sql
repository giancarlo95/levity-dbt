WITH prediction_models_prediction AS (

    SELECT * FROM {{ ref('prediction_models_prediction') }}

)

SELECT 
    account_id,
    EXTRACT(DATE FROM date_prediction_made)            AS date,
    COUNT(*)                                           AS predictions_count
FROM 
    prediction_models_prediction
GROUP BY
    account_id,
    EXTRACT(DATE FROM date_prediction_made)