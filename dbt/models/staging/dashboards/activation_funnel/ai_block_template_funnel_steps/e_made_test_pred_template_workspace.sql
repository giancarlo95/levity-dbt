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
        AND new_source = "test_tab"

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

)

SELECT
    pmp.new_workspace_id AS workspace_id,
    MIN(pmp.created_at) AS made_test_pred_at,
    CAST(MIN(pmp.created_at) AS STRING) AS made_test_pred_at_string
FROM
    pm_prediction pmp
INNER JOIN pm_classifier pmc ON pmc.new_id = pmp.new_classifier_id
INNER JOIN d_dataset dd ON dd.new_id = pmc.new_dataset_id
GROUP BY
    workspace_id

