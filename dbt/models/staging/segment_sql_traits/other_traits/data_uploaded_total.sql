WITH datasets_data AS (

    SELECT 
        user_id,
        aiblock_id,
        datapoint_id,
        date_datapoint_uploaded,
        workspace_id
    FROM 
        {{ref('datasets_data')}}

), datasets_dataset AS (

    SELECT
        user_id,
        aiblock_id
    FROM 
        {{ref('datasets_dataset')}}

)
    
SELECT 
    IFNULL(dsd.user_id, dst.user_id)    AS user_id,
    COUNT(dsd.datapoint_id)              AS data_uploaded_total
FROM datasets_data dsd
INNER JOIN datasets_dataset dst ON dsd.aiblock_id = dst.aiblock_id
GROUP BY 
    1