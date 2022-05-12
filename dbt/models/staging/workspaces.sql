WITH legacy_workspaces AS (

    SELECT
       account_id                      AS workspace_id,
       company_name                    AS workspace_name
    FROM 
       {{ref('legacy_workspaces')}}

), django_production_workspaces AS (

    SELECT 
        id                    AS workspace_id,
        name                  AS workspace_name
    FROM
        {{ref('django_production_workspaces')}}

)

SELECT * FROM legacy_workspaces UNION DISTINCT
SELECT * FROM django_production_workspaces
