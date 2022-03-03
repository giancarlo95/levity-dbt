WITH datasets_dataset AS (

    SELECT 
       user_id,
       aiblock_id
    FROM 
       {{ref('datasets_dataset')}}
    WHERE
       aiblock_description IS NULL

), datasets_data AS (

    SELECT 
       user_id,
       aiblock_id,
	   MIN(date_datapoint_uploaded) AS date_somedata_uploaded
    FROM 
       {{ref('datasets_data')}}
    GROUP BY
       user_id,
       aiblock_id

)

SELECT
	datasets_data.user_id,
    MIN(date_somedata_uploaded) AS date_first_somedata_uploaded
FROM
	datasets_data
INNER JOIN datasets_dataset ON
	datasets_data.aiblock_id = datasets_dataset.aiblock_id
GROUP BY 
    datasets_data.user_id
