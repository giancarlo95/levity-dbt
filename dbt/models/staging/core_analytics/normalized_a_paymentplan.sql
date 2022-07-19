WITH ce_a_paymentplan AS (

    SELECT
        *
    FROM
        {{ref("change_events")}} ce
    WHERE 
        table_name="accounts_paymentplan"
        AND created_at IS NOT NULL
    
)

SELECT 
    table_name,
    created_at,
    event_id,
    op,
    JSON_VALUE(content, '$.old.frontegg_tenant_id') AS old_workspace_id,
    JSON_VALUE(content, '$.old.frontegg_user_id') AS old_user_id,
    CAST(JSON_VALUE(content, '$.old.updated_at') AS TIMESTAMP) AS old_updated_at,
    CAST(JSON_VALUE(content, '$.old.created_at') AS TIMESTAMP) AS old_created_at,
    JSON_VALUE(content, '$.old.id') AS old_id,
    JSON_VALUE(content, '$.old.status') AS old_status,
    CAST(JSON_VALUE(content, '$.old.period_start') AS DATE) AS old_period_start,
    JSON_VALUE(content, '$.old.actions') AS old_actions,
    JSON_VALUE(content, '$.old.trial_period') AS old_trial_period,
    JSON_VALUE(content, '$.old.plan_id') AS old_plan_id,
    JSON_VALUE(content, '$.new.frontegg_tenant_id') AS new_workspace_id,
    JSON_VALUE(content, '$.new.frontegg_user_id') AS new_user_id,
    CAST(JSON_VALUE(content, '$.new.updated_at') AS TIMESTAMP) AS new_updated_at,
    CAST(JSON_VALUE(content, '$.new.created_at') AS TIMESTAMP) AS new_created_at,
    JSON_VALUE(content, '$.new.id') AS new_id,
    JSON_VALUE(content, '$.new.status') AS new_status,
    CAST(JSON_VALUE(content, '$.new.period_start') AS DATE) AS new_period_start,
    JSON_VALUE(content, '$.new.actions') AS new_actions,
    JSON_VALUE(content, '$.new.trial_period') AS new_trial_period,
    JSON_VALUE(content, '$.new.plan_id') AS new_plan_id,
FROM 
    ce_a_paymentplan
ORDER BY
    created_at DESC
