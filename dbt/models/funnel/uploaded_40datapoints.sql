WITH datasets_data AS (

    SELECT
		user_id,
		aiblock_id,
        MAX(date_datapoint_uploaded) as date_all_datapoints_uploaded
	FROM
		{{ref('datasets_data')}}
	GROUP BY
		user_id,
		aiblock_id
	HAVING
		COUNT(datapoint_id)>40

), datasets_dataset AS (

    SELECT 
        user_id,
        aiblock_id
    FROM {{ref('datasets_dataset')}}
    WHERE 
        aiblock_description IS NULL

)

SELECT
	datasets_data.user_id,
    MIN(datasets_data.date_all_datapoints_uploaded) AS date_first_40datapoints_uploaded
FROM
	datasets_data
INNER JOIN datasets_dataset ON
	datasets_data.aiblock_id = datasets_dataset.aiblock_id
GROUP BY
    datasets_data.user_id