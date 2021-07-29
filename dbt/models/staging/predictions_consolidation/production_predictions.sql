WITH prediction_models_prediction_full AS (

    SELECT * FROM {{ ref('prediction_models_prediction_full') }}

), onboarded_users AS (

    SELECT * FROM {{ ref('onboarded_users') }}

)

SELECT 
    account_id,
    EXTRACT(DATE FROM date_prediction_made)            AS date,
    COUNT(*)                                           AS predictions_count
FROM 
    prediction_models_prediction_full
INNER JOIN 
    onboarded_users ON onboarded_users.user_id=prediction_models_prediction_full.user_id
GROUP BY
    account_id,
    EXTRACT(DATE FROM date_prediction_made)