{{
  config(
    materialized = 'table',
    )
}}

WITH workspace_onboarded AS (

    SELECT 
        context_group_id AS workspace_id,
        email,
        MIN(uo.timestamp) AS workspace_onboarded_at
    FROM
        {{ref("django_production_user_onboarded")}} uo
    GROUP BY
        context_group_id,
        email

), 

cloned_ai_block AS (

    SELECT 
        *
    FROM
        {{ref("a_cloned_ai_block_template_workspace")}}

), 

retrained_model AS (

    SELECT 
        *
    FROM
        {{ref("d_possibly_retrained_model_workspace")}} 

), 

made_test_pred AS (

    SELECT 
        *
    FROM
        {{ref("e_made_test_pred_template_workspace")}} pd

), 

made_prod_pred AS (

    SELECT 
        *
    FROM
        {{ref("f_made_prod_pred_template_workspace")}} pd

), 

made_50_prod_pred AS (

    SELECT 
        *
    FROM
        {{ref("g_made_50_prod_pred_template_workspace")}} pd

)

SELECT
    EXTRACT(YEAR FROM workspace_onboarded_at) AS year,
    EXTRACT(MONTH FROM workspace_onboarded_at) AS month,
    FORMAT_TIMESTAMP("%b %Y", workspace_onboarded_at) AS year_month,
    *
FROM
    workspace_onboarded wo
LEFT JOIN cloned_ai_block USING(workspace_id, email)
LEFT JOIN retrained_model USING(workspace_id, email)
LEFT JOIN made_test_pred USING(workspace_id, email)
LEFT JOIN made_prod_pred USING(workspace_id, email)
LEFT JOIN made_50_prod_pred USING(workspace_id, email)
WHERE
    NOT(wo.email LIKE "%@levity.ai")
ORDER BY
    1,
    2 ASC