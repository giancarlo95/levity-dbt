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

)

SELECT * FROM legacy_workspaces UNION
SELECT * FROM django_production_workspaces
