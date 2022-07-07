{{
  config(
    materialized = 'table',
    )
}}

WITH d_dataset AS (

    SELECT 
        new_id,
        new_type,
        new_description
    FROM 
        {{ref('normalized_d_dataset')}}
    WHERE   
        op = "INSERT"

), legacy_d_dataset AS (

    SELECT 
        aiblock_id,
        aiblock_data_type
    FROM 
        {{ref('datasets_dataset')}}

), pm_classifier AS (

    SELECT 
        new_id,
        new_dataset_id
    FROM 
        {{ref('normalized_pm_classifier')}}
    WHERE   
        op = "INSERT"

), legacy_pm_classifier AS (

    SELECT 
        aiblock_id,
        classifier_id
    FROM 
        {{ref('prediction_models_classifier')}}

), pm_classifierversion AS (

    SELECT 
        *
    FROM 
        {{ref('normalized_pm_classifierversion')}}
    WHERE
        created_at IS NOT NULL

), training_start AS (

    SELECT 
        created_at AS start_time,
        new_id,
        new_classifier_id,
        new_is_template,
        new_user_id,
        new_workspace_id
    FROM 
        pm_classifierversion
    WHERE
        op = "INSERT"
        AND new_is_template = "no"

), training_end AS (

    SELECT 
        MIN(created_at) AS end_time,
        new_id,
    FROM 
        pm_classifierversion
    WHERE
        op = "UPDATE"
        AND new_status = "ready"
        AND new_training_progress = 100
    GROUP BY
        new_id

)

SELECT
    ts.new_id AS classifierversion_id,
    COALESCE(dd.new_id, ldd.aiblock_id) AS dataset_id,
    COALESCE(new_type, ldd.aiblock_data_type) AS data_type,
    CASE WHEN new_description IS NOT NULL THEN "yes" ELSE "no" END AS is_template_retraining,
    start_time,
    end_time,
    TIMESTAMP_DIFF(end_time, start_time, SECOND) AS duration,
    new_user_id AS user_id,
    new_workspace_id AS workspace_id
FROM
    training_start ts
LEFT JOIN training_end te USING(new_id)
LEFT JOIN pm_classifier pmc ON pmc.new_id = ts.new_classifier_id
LEFT JOIN d_dataset dd ON dd.new_id = pmc.new_dataset_id
LEFT JOIN legacy_pm_classifier lpmc ON lpmc.classifier_id = ts.new_classifier_id
LEFT JOIN legacy_d_dataset ldd ON ldd.aiblock_id = lpmc.aiblock_id