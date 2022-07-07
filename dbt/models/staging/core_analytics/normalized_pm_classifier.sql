WITH ce_pm_classifier AS (

    SELECT
        *
    FROM
        {{ref("change_events")}} ce
    WHERE 
        table_name="prediction_models_classifier"
    
)

SELECT 
    table_name,
    created_at,
    event_id,
    op,
    JSON_VALUE(content, '$.old.frontegg_tenant_id') AS old_workspace_id,
    JSON_VALUE(content, '$.old.dataset_id') AS old_dataset_id,
    JSON_VALUE(content, '$.old.frontegg_user_id') AS old_user_id,
    CAST(JSON_VALUE(content, '$.old.updated_at') AS TIMESTAMP) AS old_updated_at,
    CAST(JSON_VALUE(content, '$.old.created_at') AS TIMESTAMP) AS old_created_at,
    JSON_VALUE(content, '$.old.id') AS old_id,
    JSON_VALUE(content, '$.new.frontegg_tenant_id') AS new_workspace_id,
    JSON_VALUE(content, '$.new.dataset_id') AS new_dataset_id,
    JSON_VALUE(content, '$.new.frontegg_user_id') AS new_user_id,
    CAST(JSON_VALUE(content, '$.new.updated_at') AS TIMESTAMP) AS new_updated_at,
    CAST(JSON_VALUE(content, '$.new.created_at') AS TIMESTAMP) AS new_created_at,
    JSON_VALUE(content, '$.new.id') AS new_id
FROM 
    ce_pm_classifier
ORDER BY
    created_at DESC
