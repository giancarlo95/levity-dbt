WITH datasets_dataset AS (

    SELECT 
        aiblock_id
    FROM {{ref('datasets_dataset')}}
    WHERE 
        aiblock_description IS NULL

), datasets_data AS (

    SELECT 
       user_id,
       aiblock_id,
       datapoint_id,
	   date_datapoint_uploaded
    FROM 
       {{ref('datasets_data')}}

), datasets_datalabel AS (

    SELECT
        datasets_datalabel.user_id,
        datasets_data.aiblock_id,
        datasets_datalabel.datapoint_id,
        datasets_datalabel.date_labelled_datapoint_uploaded
    FROM   
        {{ref('datasets_datalabel')}}
    INNER JOIN datasets_data
        ON datasets_data.datapoint_id=datasets_datalabel.datapoint_id

), datasets_datalabel_large AS (

    SELECT 
        datasets_datalabel.aiblock_id,
        COUNT(datasets_datalabel.datapoint_id)
    FROM 
        datasets_datalabel
    GROUP BY
        datasets_datalabel.aiblock_id
    HAVING
        COUNT(datasets_datalabel.datapoint_id)>=40

), final AS (

    SELECT 
        user_id,
        datasets_datalabel.aiblock_id,
        date_labelled_datapoint_uploaded
    FROM datasets_datalabel
    INNER JOIN datasets_dataset ON 
        datasets_dataset.aiblock_id=datasets_datalabel.aiblock_id
    INNER JOIN datasets_datalabel_large ON
        datasets_datalabel_large.aiblock_id=datasets_datalabel.aiblock_id

), final_ordered AS (

    SELECT 
        user_id,
        aiblock_id,
        date_labelled_datapoint_uploaded,
        ROW_NUMBER() OVER (PARTITION BY user_id, aiblock_id ORDER BY date_labelled_datapoint_uploaded) AS RowNumber
    FROM 
        final

)

SELECT 
    user_id,
    MIN(date_labelled_datapoint_uploaded)     AS date_first_40labeldatapoints_uploaded
FROM 
    final_ordered
WHERE 
    RowNumber=40
GROUP BY
    user_id
