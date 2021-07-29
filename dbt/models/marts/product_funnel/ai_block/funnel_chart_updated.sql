{{
  config(
    materialized = 'table',
    )
}}

WITH onboarded_users AS (

    SELECT 
        user_id,
        date_user_onboarded AS timing,
        'A.Onboarded users' AS Funnel_step 
    FROM {{ref('onboarded_users')}}
       
), created_ai_block AS (

    SELECT 
        user_id,
        date_first_aiblock_created AS timing,
        'B.Created at least 1 AI Block' AS Funnel_step
    FROM {{ref('created_ai_block')}}

), uploaded_data AS (

    SELECT 
        user_id,
        date_first_somedata_uploaded AS timing,
        'C.Uploaded at least 1 Data point to at least 1 AI Block' AS Funnel_step
    FROM {{ref('uploaded_data')}}

), uploaded_40datapoints AS (

    SELECT
        user_id,
        date_first_40datapoints_uploaded AS timing,
        'D.Uploaded at least 40 Data points to at least 1 AI Block' AS Funnel_step
    FROM {{ref('uploaded_40datapoints')}}

), trained_ai_block AS (

    SELECT 
        user_id,
        date_first_training_run AS timing,
        'E.Trained at least 1 AI Block' AS Funnel_step
    FROM {{ref('trained_ai_block')}}

), made_prediction AS (

    SELECT 
        user_id,
        date_first_prediction_made AS timing,
        'F.Made at least 1 Prediction through at least 1 AI Block' AS Funnel_step
    FROM {{ref('made_prediction')}}

), made_50predictions AS (

    SELECT 
        user_id,
        date_first_50predictions_made AS timing,
        'G.Made at least 50 Predictions through at least 1 AI Block' AS Funnel_step
    FROM {{ref('made_50predictions')}}

), final AS (

    SELECT * FROM onboarded_users UNION ALL
    SELECT * FROM created_ai_block UNION ALL
    SELECT * FROM uploaded_data UNION ALL
    SELECT * FROM uploaded_40datapoints UNION ALL
    SELECT * FROM trained_ai_block UNION ALL
    SELECT * FROM made_prediction UNION ALL
    SELECT * FROM made_50predictions
    
) 

SELECT
    COUNT(user_id) AS Number_of_customers,
    Funnel_step
FROM 
    final
WHERE 
    user_id in (SELECT user_id FROM onboarded_users)
GROUP BY 
    Funnel_step
ORDER BY
    Funnel_step