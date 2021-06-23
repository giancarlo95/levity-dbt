WITH datasets_dataset AS (

    SELECT 
       user_id,
       aiblock_id
    FROM 
       {{ref('datasets_dataset')}}
    WHERE
       aiblock_description IS NULL

), datasets_data AS (

    SELECT * FROM {{ref('datasets_data')}}

), datasets_datalabel AS (

    SELECT * FROM {{ref('datasets_datalabel')}}

), datasets_data_filtered AS (

    SELECT
    	datasets_data.datapoint_id
    FROM
    	datasets_data
    INNER JOIN datasets_dataset ON
    	datasets_data.aiblock_id = datasets_dataset.aiblock_id

)

SELECT
    datasets_datalabel.user_id,
    MIN(date_labelled_datapoint_uploaded) AS date_first_labelled_datapoint_uploaded
FROM   
    datasets_datalabel
INNER JOIN datasets_data_filtered
    ON datasets_data_filtered.datapoint_id=datasets_datalabel.datapoint_id
GROUP BY 
    datasets_datalabel.user_id