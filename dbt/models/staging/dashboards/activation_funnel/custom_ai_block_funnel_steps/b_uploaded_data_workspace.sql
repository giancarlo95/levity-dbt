{{
  config(
    materialized = 'table',
    )
}}

WITH userflow_ai_blocks AS (

    SELECT
        *
    FROM
        {{ref('userflow_ai_blocks')}}

), d_dataset AS (

    SELECT 
        * 
    FROM 
        {{ref('normalized_d_dataset')}} ndd
    LEFT JOIN userflow_ai_blocks uab ON ndd.new_id = uab.dataset_id 
    WHERE 
        op = "INSERT"
        AND new_description IS NULL
        AND is_userflow_data IS NULL

), d_data AS (

    SELECT
        *
    FROM
        {{ref('normalized_d_data')}}
    WHERE 
        op = "INSERT"

)

SELECT
	dd.new_workspace_id AS workspace_id,
	MIN(dd.created_at) AS data_added_at,
    CAST(MIN(dd.created_at) AS STRING) AS data_added_at_string
FROM
	d_data dd
INNER JOIN d_dataset dds ON dds.new_id = dd.new_dataset_id
GROUP BY
	workspace_id