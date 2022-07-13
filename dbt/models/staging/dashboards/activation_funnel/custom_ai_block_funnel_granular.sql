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

added_40dp AS (

    SELECT 
        *
    FROM
        {{ref("c_uploaded_40datapoints_workspace")}} 

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

made_prod_pred AS (

    SELECT 
        *
    FROM
        {{ref("f_made_prod_pred_workspace")}} pd

), 

made_50_prod_pred AS (

    SELECT 
        *
    FROM
        {{ref("g_made_50_prod_pred_workspace")}} pd

)

SELECT
    EXTRACT(YEAR FROM onboarded_at) AS year,
    EXTRACT(MONTH FROM onboarded_at) AS month,
    FORMAT_TIMESTAMP("%b %Y", onboarded_at) AS year_month,
    *
FROM
    workspace_onboarded wo
LEFT JOIN created_ai_block USING(workspace_id, email)
LEFT JOIN data_added USING(workspace_id, email)
LEFT JOIN added_40dp USING(workspace_id, email)
LEFT JOIN trained_ai_block USING(workspace_id, email)
LEFT JOIN made_test_pred USING(workspace_id, email)
LEFT JOIN made_prod_pred USING(workspace_id, email)
LEFT JOIN made_50_prod_pred USING(workspace_id, email)
WHERE
    NOT(wo.email LIKE "%@levity.ai")
ORDER BY
    onboarded_at ASC
