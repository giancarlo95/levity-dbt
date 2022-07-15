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

), d_data AS (

    SELECT
        *
    FROM
        {{ref('normalized_d_data')}}
    WHERE
        op = "INSERT"

)

SELECT
	dds.new_id AS dataset_id,
    "yes" AS is_userflow_data
FROM
	d_data dd
INNER JOIN d_dataset dds ON dds.new_id = dd.new_dataset_id
WHERE 
    dds.new_description IS NULL 
    AND (dd.new_text = "Yes, I would like to learn more about your product. I have some questions about the so that if we meet up I can ask them. My company is also looking for a solution like this. Can we schedule a meeting?" 
        OR dd.new_original_file_name = "Bedroom%" OR dd.new_original_file_name = "Bathroom%")
