{{
  config(
    materialized = 'table',
    )
}}

WITH workspace_onboarded AS (

    SELECT 
        context_group_id AS workspace_id,
        email,
        MIN(uo.timestamp) AS onboarded_at,
        CAST(MIN(uo.timestamp) AS STRING) AS onboarded_at_string,
    FROM
        {{ref("django_production_user_onboarded")}} uo
    GROUP BY
        context_group_id,
        email

), 

created_ai_block AS (

    SELECT 
        *
    FROM
        {{ref("a_created_ai_block_workspace")}}

), 

data_added AS (

    SELECT 
        *
    FROM
        {{ref("b_uploaded_data_workspace")}} 

), 

trained_ai_block AS (

    SELECT 
        *
    FROM
        {{ref("d_trained_ai_block_workspace")}} 

), 

made_test_pred AS (

    SELECT 
        *
    FROM
        {{ref("e_made_test_pred_workspace")}} pd

),

made_50_prod_pred AS (

    SELECT 
        *
    FROM
        {{ref("g_made_50_prod_pred_workspace")}} pd

)

SELECT
    email,
    CASE
        WHEN made_50_prod_pred_at IS NOT NULL THEN "made_50+_predictions"
        WHEN made_test_pred_at IS NOT NULL THEN "predictions_50_nudge"
        WHEN trained_ai_block_at IS NOT NULL THEN "predictions_1_nudge"
        WHEN data_added_at IS NOT NULL THEN "ai_block_trained_nudge"
        WHEN created_ai_block_at IS NOT NULL THEN "datapoints_added_nudge"
        ELSE "ai_block_created_nudge"
    END AS custom_ai_block_funnel_activation_drip_audiences
FROM
    workspace_onboarded wo
LEFT JOIN created_ai_block USING(workspace_id, email)
LEFT JOIN data_added USING(workspace_id, email)
LEFT JOIN trained_ai_block USING(workspace_id, email)
LEFT JOIN made_test_pred USING(workspace_id, email)
LEFT JOIN made_50_prod_pred USING(workspace_id, email)
WHERE
    NOT(wo.email LIKE "%@levity.ai")



