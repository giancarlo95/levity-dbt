WITH datasets_data AS (

    SELECT 
        user_id,
        aiblock_id,
        datapoint_id,
        date_datapoint_uploaded,
        account_id
    FROM 
        {{ref('datasets_data')}}

), datasets_dataset AS (

    SELECT
        user_id,
        aiblock_id
    FROM {{ref('datasets_dataset')}}

), final AS (
    
    SELECT 
        IFNULL(dsd.user_id, dst.user_id) AS user_id,
        dsd.aiblock_id AS aiblock_id,
        dsd.account_id AS company_id,
        COUNT(dsd.datapoint_id) AS net_data_points, 
        DATE_TRUNC(dsd.date_datapoint_uploaded, DAY) AS time_stamp 
    FROM datasets_data dsd
    INNER JOIN datasets_dataset dst ON dsd.aiblock_id = dst.aiblock_id
    GROUP BY 1, 2, 3, 5
    ORDER BY 4 DESC

)


SELECT 
    *
FROM final
