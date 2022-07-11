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

), d_data AS (

    SELECT 
        *,
        CASE 
            WHEN op = "INSERT" THEN new_dataset_id
            ELSE old_dataset_id
        END AS dataset_id
    FROM 
        {{ref('normalized_d_data')}}
    WHERE
        created_at IS NOT NULL

), legacy_d_data AS (

    SELECT 
        aiblock_id,
        date_datapoint_uploaded
    FROM 
        {{ref('datasets_data')}}
    WHERE 
        date_datapoint_updated < "2022-06-29 13:35:27.198000 UTC"

),pm_classifier AS (

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

), d_data_calc AS (

    SELECT
        dataset_id,
        COUNT(CASE WHEN op = "INSERT" THEN 1 END) - COUNT(CASE WHEN op = "DELETE" THEN 1 END) AS data_count
    FROM
        d_data dd
    INNER JOIN pm_classifier pmc ON dd.dataset_id = pmc.new_dataset_id
    INNER JOIN legacy_pm_classifier lpmc ON dd.dataset_id = lpmc.aiblock_id
    INNER JOIN training_start ts ON ts.new_classifier_id = COALESCE(pmc.new_id, lpmc.classifier_id)
    WHERE 
        dd.created_at < ts.start_time
    GROUP BY
        dd.dataset_id

), legacy_d_data_calc AS (

    SELECT
        ldd.aiblock_id AS dataset_id,
        COUNT(*) AS legacy_data_count
    FROM
        legacy_d_data ldd
    INNER JOIN legacy_pm_classifier lpmc ON ldd.aiblock_id = lpmc.aiblock_id
    INNER JOIN training_start ts ON ts.new_classifier_id = lpmc.classifier_id
    GROUP BY
        ldd.aiblock_id

)

SELECT
    ts.new_id AS classifierversion_id,
    COALESCE(dd.new_id, ldd.aiblock_id) AS dataset_id,
    COALESCE(new_type, ldd.aiblock_data_type) AS data_type,
    CASE WHEN new_description IS NOT NULL THEN "yes" ELSE "no" END AS is_template_retraining,
    EXTRACT(YEAR FROM start_time) AS start_year,
    EXTRACT(WEEK FROM start_time) AS start_week,
    CAST(start_time AS DATE) AS start_day,
    start_time,
    end_time,
    TIMESTAMP_DIFF(end_time, start_time, SECOND) AS duration_seconds,
    COALESCE(data_count, 0) + COALESCE(legacy_data_count, 0) AS data_count,
    new_user_id AS user_id,
    new_workspace_id AS workspace_id
FROM
    training_start ts
LEFT JOIN training_end te USING(new_id)
LEFT JOIN pm_classifier pmc ON pmc.new_id = ts.new_classifier_id
LEFT JOIN d_dataset dd ON dd.new_id = pmc.new_dataset_id
LEFT JOIN legacy_pm_classifier lpmc ON lpmc.classifier_id = ts.new_classifier_id
LEFT JOIN legacy_d_dataset ldd ON ldd.aiblock_id = lpmc.aiblock_id
LEFT JOIN d_data_calc ddc ON COALESCE(pmc.new_dataset_id, lpmc.aiblock_id) = ddc.dataset_id
LEFT JOIN legacy_d_data_calc lddc ON lddc.dataset_id = lpmc.aiblock_id
ORDER BY
    start_time DESC