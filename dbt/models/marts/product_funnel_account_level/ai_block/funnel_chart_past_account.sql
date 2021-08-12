{{
  config(
    materialized = 'table',
    )
}}

WITH onboarded_accounts AS (

    SELECT 
        account_id,
        date_account_onboarded AS timing,
        'A.Onboarded accounts' AS Funnel_step 
    FROM {{ref('onboarded_accounts')}}
       
), created_ai_block AS (

    SELECT 
        account_id,
        date_first_aiblock_created AS timing,
        'B.Created at least 1 AI Block' AS Funnel_step
    FROM {{ref('created_ai_block_account')}}

), uploaded_data AS (

    SELECT 
        account_id,
        date_first_somedata_uploaded AS timing,
        'C.Uploaded at least 1 Data point to at least 1 AI Block' AS Funnel_step
    FROM {{ref('uploaded_data_account')}}

), uploaded_40datapoints AS (

    SELECT
        account_id,
        date_first_40datapoints_uploaded AS timing,
        'D.Uploaded at least 40 Data points to at least 1 AI Block' AS Funnel_step
    FROM {{ref('uploaded_40datapoints_account')}}

), trained_ai_block AS (

    SELECT 
        account_id,
        date_first_training_run AS timing,
        'E.Trained at least 1 AI Block' AS Funnel_step
    FROM {{ref('trained_ai_block_account')}}

), made_prediction AS (

    SELECT 
        account_id,
        date_first_prediction_made AS timing,
        'F.Made at least 1 Prediction through at least 1 AI Block' AS Funnel_step
    FROM {{ref('made_prediction_account')}}

), made_50predictions AS (

    SELECT 
        account_id,
        date_first_50predictions_made AS timing,
        'G.Made at least 50 Predictions through at least 1 AI Block' AS Funnel_step
    FROM {{ref('made_50predictions_account')}}

), final AS (

    SELECT * FROM onboarded_accounts UNION ALL
    SELECT * FROM created_ai_block UNION ALL
    SELECT * FROM uploaded_data UNION ALL
    SELECT * FROM uploaded_40datapoints UNION ALL
    SELECT * FROM trained_ai_block UNION ALL
    SELECT * FROM made_prediction UNION ALL
    SELECT * FROM made_50predictions
    
) 

SELECT
    COUNT(account_id) AS Number_of_customers,
    Funnel_step
FROM 
    final
WHERE 
    account_id in (SELECT account_id FROM onboarded_accounts)
    AND TIMESTAMP_DIFF(timing, TIMESTAMP("2021-06-01 00:00:00+00"), DAY)<0
GROUP BY
    Funnel_step
ORDER BY
    Funnel_step
