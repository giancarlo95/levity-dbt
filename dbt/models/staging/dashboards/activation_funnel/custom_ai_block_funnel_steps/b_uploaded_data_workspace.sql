{{
  config(
    materialized = 'table',
    )
}}

WITH d_dataset AS (

    SELECT 
        * 
    FROM 
        {{ref('normalized_d_dataset')}}
    WHERE 
        op = "INSERT"
        AND new_description IS NULL

), workspaces AS (

    SELECT 
        context_group_id AS workspace_id,
        email,
    FROM
        {{ref("django_production_user_onboarded")}} uo
    WHERE 
        NOT(email LIKE "%@levity.ai")
    GROUP BY
        context_group_id,
        email

), d_data AS (

    SELECT
        *
    FROM
        {{ref('normalized_d_data')}}
    WHERE 
        op = "INSERT"
)

SELECT
	workspace_id,
    email,
	min(dd.created_at) AS data_added_at
FROM
	d_data dd
INNER JOIN d_dataset dds ON dds.new_id = dd.new_dataset_id
INNER JOIN workspaces w ON dd.new_workspace_id = w.workspace_id
GROUP BY
	workspace_id,
    email