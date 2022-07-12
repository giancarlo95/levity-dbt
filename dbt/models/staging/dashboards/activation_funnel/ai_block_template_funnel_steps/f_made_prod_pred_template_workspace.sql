{{
  config(
    materialized = 'table',
    )
}}

WITH pm_prediction AS (

    SELECT
        *
	FROM
		{{ref('normalized_pm_prediction')}}
    WHERE
        op = "INSERT"
        AND NOT(new_source = "test_tab")

), d_dataset AS (

    SELECT 
        *
    FROM 
        {{ref('normalized_d_dataset')}}
    WHERE   
        op = "INSERT"
        AND new_description IS NOT NULL

), pm_classifier AS (

    SELECT 
        *
    FROM 
        {{ref('normalized_pm_classifier')}}
    WHERE   
        op = "INSERT"

), workspaces AS (

    SELECT 
        context_group_id AS workspace_id,
        email,
    FROM
        {{ref("django_production_user_onboarded")}} uo
    WHERE 
        NOT(email LIKE "%@levity.ai")
    GROUP BY
        context_group_id,
        email

)

SELECT
    workspace_id,
    email,
    MIN(pmp.created_at) AS made_prod_pred_at,
    CAST(MIN(pmp.created_at) AS STRING) AS made_prod_pred_at_string
FROM
    pm_prediction pmp
INNER JOIN pm_classifier pmc ON pmc.new_id = pmp.new_classifier_id
INNER JOIN d_dataset dd ON dd.new_id = pmc.new_dataset_id
INNER JOIN workspaces w ON w.workspace_id = pmp.new_workspace_id
GROUP BY
    workspace_id,
    email


