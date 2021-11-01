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

), onboarded_users AS (

    SELECT
        user_id,
        user_email_address
    FROM
        {{ref('onboarded_users')}}
        
), final AS (
    
    SELECT 
        IFNULL(dsd.user_id, dst.user_id) AS user_id,
        dsd.aiblock_id AS aiblock_id,
        dsd.account_id AS company_id,
        COUNT(dsd.datapoint_id) AS net_data_points, 
        DATE_TRUNC(dsd.date_datapoint_uploaded, DAY) AS time_stamp,
        user_email_address 
    FROM datasets_data dsd
    INNER JOIN datasets_dataset dst ON dsd.aiblock_id = dst.aiblock_id
    GROUP BY 1, 2, 3, 5, 6
    ORDER BY 4 DESC

)


SELECT 
    final.user_id,
    aiblock_id,
    company_id,
    net_data_points,
    time_stamp,
    user_email_address
FROM final
INNER JOIN onboarded_users ob ON final.user_id = ob.user_id
WHERE TIMESTAMP_DIFF(TIMESTAMP "2021-10-28 23:59:59+00", time_stamp, HOUR)>0
