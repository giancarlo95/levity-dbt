WITH prediction_models_prediction AS (

    SELECT * FROM {{ ref('prediction_models_prediction') }}

), onboarded_accounts AS (

    SELECT * FROM {{ ref('onboarded_accounts') }}

)

SELECT 
    prediction_models_prediction.account_id,
    EXTRACT(DATE FROM date_prediction_made)            AS date,
    COUNT(*)                                           AS predictions_count
FROM 
    prediction_models_prediction
INNER JOIN 
    onboarded_accounts ON onboarded_accounts.logged_account_id=prediction_models_prediction.account_id
GROUP BY
    prediction_models_prediction.account_id,
    EXTRACT(DATE FROM date_prediction_made)