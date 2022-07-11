WITH ce_d_dataset AS (

    SELECT
        *
    FROM
        {{ref("change_events")}} ce
    WHERE 
        table_name="datasets_dataset"
        AND created_at IS NOT NULL
    
)

SELECT 
    table_name,
    created_at,
    event_id,
    op,
    JSON_VALUE(content, '$.old.frontegg_tenant_id') AS old_workspace_id,
    JSON_VALUE(content, '$.old.frontegg_user_id') AS old_user_id,
    JSON_VALUE(content, '$.old.name') AS old_name,
    CAST(JSON_VALUE(content, '$.old.updated_at') AS TIMESTAMP) AS old_updated_at,
    CAST(JSON_VALUE(content, '$.old.created_at') AS TIMESTAMP) AS old_created_at,
    JSON_VALUE(content, '$.old.id') AS old_id,
    JSON_VALUE(content, '$.old.type') AS old_type,
    CASE WHEN JSON_VALUE(content, '$.old.template') = "false" THEN "no" WHEN JSON_VALUE(content, '$.old.template') = "true" THEN "yes" END AS old_is_template,
    JSON_VALUE(content, '$.old.description') AS old_description,
    JSON_VALUE(content, '$.new.frontegg_tenant_id') AS new_workspace_id,
    JSON_VALUE(content, '$.new.frontegg_user_id') AS new_user_id,
    JSON_VALUE(content, '$.new.name') AS new_name,
    CAST(JSON_VALUE(content, '$.new.updated_at') AS TIMESTAMP) AS new_updated_at,
    CAST(JSON_VALUE(content, '$.new.created_at') AS TIMESTAMP) AS new_created_at,
    JSON_VALUE(content, '$.new.id') AS new_id,
    JSON_VALUE(content, '$.new.type') AS new_type,
    CASE WHEN JSON_VALUE(content, '$.new.template') = "false" THEN "no" WHEN JSON_VALUE(content, '$.new.template') = "true" THEN "yes" END AS new_is_template,
    JSON_VALUE(content, '$.new.description') AS new_description
FROM 
    ce_d_dataset
ORDER BY
    created_at DESC
