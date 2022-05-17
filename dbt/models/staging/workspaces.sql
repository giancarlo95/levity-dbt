WITH legacy_workspaces AS (

    SELECT
       account_id                      AS workspace_id,
       company_name                    AS workspace_name,
       date_account_onboarded          AS workspace_created_at
    FROM 
       {{ref('legacy_workspaces')}}

), django_production_workspaces AS (

    SELECT 
        id                    AS workspace_id,
        name                  AS workspace_name,
        received_at           AS workspace_created_at
    FROM
        {{ref('django_production_workspaces')}}

), unioned AS (

    SELECT workspace_id, workspace_name FROM legacy_workspaces UNION DISTINCT
    SELECT workspace_id, workspace_name FROM django_production_workspaces

)

SELECT 
    u.workspace_id,
    u.workspace_name,
    COALESCE(l.workspace_created_at, d.workspace_created_at) AS workspace_created_at
FROM 
    unioned u
LEFT JOIN legacy_workspaces l USING(workspace_id)
LEFT JOIN django_production_workspaces d USING(workspace_id)