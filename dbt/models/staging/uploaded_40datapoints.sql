WITH datasets_data_large AS (

    SELECT
		aiblock_id
	FROM
		{{ref('datasets_data')}}
	GROUP BY
		aiblock_id
	HAVING
		COUNT(datapoint_id)>=40

), datasets_dataset AS (

    SELECT 
        aiblock_id
    FROM {{ref('datasets_dataset')}}
    WHERE 
        aiblock_description IS NULL

), datasets_data AS (

    SELECT 
       user_id,
       aiblock_id,
	   date_datapoint_uploaded
    FROM 
       {{ref('datasets_data')}}

), final AS (

    SELECT 
        user_id,
        datasets_data.aiblock_id,
        date_datapoint_uploaded
    FROM datasets_data
    INNER JOIN datasets_dataset ON 
        datasets_dataset.aiblock_id=datasets_data.aiblock_id
    INNER JOIN datasets_data_large ON
        datasets_data_large.aiblock_id=datasets_data.aiblock_id

), final_ordered AS (

    SELECT 
        user_id,
        aiblock_id,
        date_datapoint_uploaded,
        ROW_NUMBER() OVER (PARTITION BY user_id, aiblock_id ORDER BY date_datapoint_uploaded) AS RowNumber
    FROM 
        final

)

SELECT 
    user_id,
    MIN(date_datapoint_uploaded)     AS date_first_40datapoints_uploaded
FROM 
    final_ordered
WHERE 
    RowNumber=40
GROUP BY
    user_id
