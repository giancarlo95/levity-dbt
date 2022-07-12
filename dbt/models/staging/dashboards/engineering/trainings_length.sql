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

), legacy_d_data AS (

    SELECT 
        aiblock_id,
        date_datapoint_uploaded,
        datapoint_text
    FROM 
        {{ref('datasets_data')}}
    WHERE 
        date_datapoint_uploaded < "2022-06-29 13:35:27.198000 UTC"

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
    WHERE
        created_at < "2022-06-29 13:34:35.727000 UTC"

), pm_classifierversion AS (

    SELECT 
        *
    FROM 
        {{ref('normalized_pm_classifierversion')}}

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
        ts.new_id,
        COUNT(*) AS data_count,
        AVG(CHAR_LENGTH(ddi.new_text)) AS avg_data_size
    FROM
        training_start ts
    LEFT JOIN pm_classifier pmc ON pmc.new_id = ts.new_classifier_id
    LEFT JOIN legacy_pm_classifier lpmc ON lpmc.classifier_id = ts.new_classifier_id
    INNER JOIN d_data ddi ON ddi.dataset_id = COALESCE(pmc.new_dataset_id, lpmc.aiblock_id) AND ddi.created_at < ts.start_time AND ddi.op = "INSERT"
    LEFT JOIN d_data ddd ON ddd.old_id = ddi.new_id AND ddd.created_at < ts.start_time AND ddd.op = "DELETE"
    WHERE 
        ddd.old_id IS NULL
    GROUP BY
        ts.new_id

), legacy_d_data_calc AS (

    SELECT
        ts.new_id,
        COUNT(*) AS legacy_data_count,
        AVG(CHAR_LENGTH(datapoint_text)) AS legacy_avg_data_size
    FROM
        training_start ts
    LEFT JOIN pm_classifier pmc ON pmc.new_id = ts.new_classifier_id
    LEFT JOIN legacy_pm_classifier lpmc ON lpmc.classifier_id = ts.new_classifier_id
    INNER JOIN legacy_d_data ldd ON ldd.aiblock_id = COALESCE(pmc.new_dataset_id, lpmc.aiblock_id) AND ldd.date_datapoint_uploaded < ts.start_time
    GROUP BY
        ts.new_id

), users AS (

    SELECT 
        id,
        email
    FROM
        {{ref('django_production_users')}}

    
), final AS (

    SELECT
        email,
        new_user_id AS user_id,
        new_workspace_id AS workspace_id,
        COALESCE(dd.new_id, ldd.aiblock_id) AS dataset_id,
        ts.new_id AS classifierversion_id,
        EXTRACT(YEAR FROM start_time) AS start_year,
        EXTRACT(WEEK FROM start_time) AS start_week,
        CAST(start_time AS DATE) AS start_day,
        start_time,
        end_time,
        COALESCE(new_type, ldd.aiblock_data_type) AS data_type,
        CASE WHEN new_description IS NOT NULL THEN "yes" ELSE "no" END AS is_template_retraining,
        TIMESTAMP_DIFF(end_time, start_time, SECOND) AS duration_seconds,
        COALESCE(data_count, 0) + COALESCE(legacy_data_count, 0) AS data_count,
        (COALESCE(avg_data_size, 0)*COALESCE(data_count, 0) + COALESCE(legacy_avg_data_size, 0)*COALESCE(legacy_data_count, 0))/(COALESCE(data_count, 0)+COALESCE(legacy_data_count, 0)) AS avg_data_size
    FROM
        training_start ts
    LEFT JOIN training_end te USING(new_id)
    LEFT JOIN pm_classifier pmc ON pmc.new_id = ts.new_classifier_id
    LEFT JOIN d_dataset dd ON dd.new_id = pmc.new_dataset_id
    LEFT JOIN legacy_pm_classifier lpmc ON lpmc.classifier_id = ts.new_classifier_id
    LEFT JOIN legacy_d_dataset ldd ON ldd.aiblock_id = lpmc.aiblock_id
    LEFT JOIN d_data_calc ddc ON ddc.new_id = ts.new_id
    LEFT JOIN legacy_d_data_calc lddc ON lddc.new_id = ts.new_id
    LEFT JOIN users u ON u.id = ts.new_user_id

)

SELECT 
    * EXCEPT(avg_data_size),
    CASE 
        WHEN NOT(data_type = "text") THEN NULL
        ELSE avg_data_size
    END AS avg_data_size
FROM
    final
ORDER BY
    start_time DESC