{{
  config(
    materialized = 'table',
    )
}}

WITH workspace_onboarded AS (

    SELECT 
        workspace_id,
        email,
        onboarded_at,
        CAST(onboarded_at AS STRING) AS onboarded_at_string,
    FROM
        {{ref("subscriptions")}}

), 

cloned_template AS (

    SELECT 
        *
    FROM
        {{ref("a_cloned_template_workspace")}}

), 

retrained_model AS (

    SELECT 
        *
    FROM
        {{ref("d_retrained_model_workspace")}} 

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
    EXTRACT(YEAR FROM onboarded_at) AS year,
    EXTRACT(MONTH FROM onboarded_at) AS month,
    FORMAT_TIMESTAMP("%b %Y", onboarded_at) AS year_month,
    *
FROM
    workspace_onboarded wo
LEFT JOIN cloned_template USING(workspace_id)
LEFT JOIN retrained_model USING(workspace_id)
LEFT JOIN made_test_pred USING(workspace_id)
LEFT JOIN made_prod_pred USING(workspace_id)
LEFT JOIN made_50_prod_pred USING(workspace_id)
ORDER BY
    onboarded_at ASC