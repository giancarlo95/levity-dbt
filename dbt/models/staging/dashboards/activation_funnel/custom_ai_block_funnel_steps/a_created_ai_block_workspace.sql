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

)

SELECT
	workspace_id,
    email,
	MIN(created_at) AS ai_block_created_at,
    CAST(MIN(created_at) AS STRING) AS ai_block_created_at_string
FROM
	d_dataset dd
INNER JOIN workspaces w ON dd.new_workspace_id = w.workspace_id
WHERE
	op = "INSERT" 
    AND new_description IS NULL
GROUP BY
	workspace_id,
    email