WITH datasets_data AS (

    SELECT 
        user_id,
        aiblock_id,
        datapoint_id,
        date_datapoint_uploaded,
        account_id
    FROM 
        {{ref('datasets_data')}}

), onboarded_users AS (

<<<<<<< HEAD
    SELECT
        user_id,
        user_email_address
=======
    SELECT 
        user_id,
        aiblock_id
>>>>>>> 70404971c39a5e1781675504ed0a4d962b05d253
    FROM
        {{ref('onboarded_users')}}
),


datasets_dataset AS (

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
        MAX(dsd.date_datapoint_uploaded) AS time_stamp
    FROM datasets_data dsd
    INNER JOIN datasets_dataset dst ON dsd.aiblock_id = dst.aiblock_id
    WHERE DATE(TIMESTAMP_TRUNC(dsd.date_datapoint_uploaded, DAY)) = DATE_SUB(CURRENT_DATE(),INTERVAL 1 DAY)
    GROUP BY 1, 2, 3 
    ORDER BY 4 DESC

)


SELECT 
<<<<<<< HEAD
    final.user_id,
    aiblock_id, 
    company_id,
    obu.user_email_address,
    net_data_points,
    time_stamp
FROM final
INNER JOIN onboarded_users obu ON final.user_id = obu.user_id
=======
    *
FROM 
    final
WHERE 
    DATE(TIMESTAMP_TRUNC(time_stamp, DAY)) = DATE_SUB(CURRENT_DATE(),INTERVAL 1 DAY)
>>>>>>> 70404971c39a5e1781675504ed0a4d962b05d253
