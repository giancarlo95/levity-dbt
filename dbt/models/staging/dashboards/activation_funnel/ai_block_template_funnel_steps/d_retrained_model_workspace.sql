{{
  config(
    materialized = 'table',
    )
}}

WITH d_dataset AS (

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

), pm_classifierversion AS (

    SELECT 
        *
    FROM 
        {{ref('normalized_pm_classifierversion')}}
    WHERE
        op = "UPDATE"
        AND new_status = "ready"
        AND new_training_progress = 100

), training_end AS (

    SELECT 
        MIN(pmcv.created_at) AS end_time,
        pmcv.new_workspace_id
    FROM 
        pm_classifierversion pmcv
    INNER JOIN pm_classifier pmc ON pmc.new_id = pmcv.new_classifier_id
    INNER JOIN d_dataset dd ON dd.new_id = pmc.new_dataset_id
    GROUP BY
        pmcv.new_workspace_id

)

SELECT
    new_workspace_id AS workspace_id,
    end_time AS retrained_model_at,
    CAST(end_time AS STRING) AS retrained_model_at_string
FROM
    training_end te
