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

)

SELECT
	new_workspace_id AS workspace_id,
	MIN(created_at) AS cloned_template_at,
    CAST(MIN(created_at) AS STRING) AS cloned_template_at_string
FROM
	d_dataset dd
WHERE
	op = "INSERT" 
    AND new_description IS NOT NULL
GROUP BY
	workspace_id