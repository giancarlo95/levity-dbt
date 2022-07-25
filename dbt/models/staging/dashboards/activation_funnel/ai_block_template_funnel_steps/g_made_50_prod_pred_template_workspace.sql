{{
  config(
    materialized = 'table',
    )
}}

WITH pm_prediction AS (

    SELECT
        created_at,
        new_source AS source,
        new_workspace_id AS workspace_id,
        new_classifier_id AS classifier_id
	FROM
		{{ref('normalized_pm_prediction')}}
    WHERE
        op = "INSERT"

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

), count_50_pred AS (

    SELECT
        workspace_id,
        classifier_id,
        COUNT(CASE WHEN NOT(source = "test_tab") THEN 1 END) AS pred_count
    FROM
        pm_prediction pmp
    INNER JOIN pm_classifier pmc ON pmc.new_id = pmp.classifier_id
    INNER JOIN d_dataset dd ON dd.new_id = pmc.new_dataset_id
    WHERE
        classifier_id IS NOT NULL
        AND workspace_id IS NOT NULL
    GROUP BY
        workspace_id,
        classifier_id
    HAVING
        COUNT(CASE WHEN NOT(source = "test_tab") THEN 1 END)>50

), time_50_pred AS (

    SELECT
        workspace_id,
        classifier_id,
        created_at,
        ROW_NUMBER() OVER(PARTITION BY workspace_id, classifier_id ORDER BY created_at ASC) AS index
    FROM
        pm_prediction
    WHERE 
        workspace_id IN (SELECT workspace_id FROM count_50_pred)
    
)

SELECT
    workspace_id,
    MIN(created_at) AS made_50_prod_pred_at,
    CAST(MIN(created_at) AS STRING) AS made_50_prod_pred_at_string
FROM
    time_50_pred
WHERE
    index = 50
GROUP BY
    workspace_id


