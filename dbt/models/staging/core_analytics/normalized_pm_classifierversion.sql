{{
  config(
    materialized = 'table',
    )
}}

WITH ce_pm_classifierversion AS (

    SELECT
        *
    FROM
        {{ref("change_events")}} ce
    WHERE 
        table_name="prediction_models_classifierversion"
    
)

SELECT 
    table_name,
    created_at
    event_id,
    op,
    JSON_VALUE(content, '$.old.status') AS old_status,
    CAST(JSON_VALUE(content, '$.old.performance_score') AS FLOAT64) AS old_performance_score,
    CAST(JSON_VALUE(content, '$.old.training_progress') AS FLOAT64) AS old_training_progress,
    JSON_VALUE(content, '$.old.frontegg_tenant_id') AS old_workspace_id,
    JSON_VALUE(content, '$.old.classifier_id') AS old_classifier_id,
    JSON_VALUE(content, '$.old.frontegg_user_id') AS old_user_id,
    CAST(JSON_VALUE(content, '$.old.seconds_left') AS FLOAT64) AS old_seconds_left,
    CAST(JSON_VALUE(content, '$.old.updated_at') AS TIMESTAMP) AS old_updated_at,
    CAST(JSON_VALUE(content, '$.old.created_at') AS TIMESTAMP) AS old_created_at,
    JSON_VALUE(content, '$.old.id') AS old_id,
    CASE WHEN JSON_VALUE(content, '$.old.template') = "false" THEN "no" ELSE "yes" END AS old_is_template,
    JSON_VALUE(content, '$.new.status') AS new_status,
    CAST(JSON_VALUE(content, '$.new.performance_score') AS FLOAT64) AS new_performance_score,
    CAST(JSON_VALUE(content, '$.new.training_progress') AS FLOAT64) AS new_training_progress,
    JSON_VALUE(content, '$.new.frontegg_tenant_id') AS new_workspace_id,
    JSON_VALUE(content, '$.new.classifier_id') AS new_classifier_id,
    JSON_VALUE(content, '$.new.frontegg_user_id') AS new_user_id,
    CAST(JSON_VALUE(content, '$.new.seconds_left') AS FLOAT64) AS new_seconds_left,
    CAST(JSON_VALUE(content, '$.new.updated_at') AS TIMESTAMP) AS new_updated_at,
    CAST(JSON_VALUE(content, '$.new.created_at') AS TIMESTAMP) AS new_created_at,
    JSON_VALUE(content, '$.new.id') AS new_id,
    CASE WHEN JSON_VALUE(content, '$.new.template') = "false" THEN "no" ELSE "yes" END AS new_is_template
FROM 
    ce_pm_classifierversion
ORDER BY
    created_at DESC
