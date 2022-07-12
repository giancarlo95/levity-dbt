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

), count_40_dp AS (

    SELECT
        dd.new_workspace_id,
        dd.new_dataset_id,
        COUNT(*) AS dp_count
    FROM
        d_data dd
    INNER JOIN d_dataset dds ON dds.new_id = dd.new_dataset_id
    GROUP BY   
        dd.new_workspace_id,
        dd.new_dataset_id
    HAVING
        COUNT(*)>40

), time_40_dp AS (

    SELECT
        dd.new_workspace_id AS workspace_id,
        dd.new_dataset_id,
        dd.created_at,
        ROW_NUMBER() OVER(PARTITION BY dd.new_workspace_id, dd.new_dataset_id ORDER BY dd.created_at ASC) AS index
    FROM
        d_data dd
    INNER JOIN d_dataset dds ON dds.new_id = dd.new_dataset_id
    WHERE 
        dd.new_workspace_id IN (SELECT new_workspace_id FROM count_40_dp)
    
)

SELECT
    workspace_id,
    email,
    MIN(created_at) AS added_40dp_at,
    CAST(MIN(created_at) AS STRING) AS added_40dp_at_string
FROM
    time_40_dp
INNER JOIN workspaces USING(workspace_id)
WHERE
    index = 40
GROUP BY
    workspace_id,
    email



