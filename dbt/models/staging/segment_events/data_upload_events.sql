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
        account_id,
        aiblock_id,
        is_template
    FROM {{ref('datasets_dataset')}}

), onboarded_accounts AS (

    SELECT
        account_id,
        sample_user
    FROM
        {{ref('onboarded_accounts')}}
        
), final AS (
    
    SELECT 
        IFNULL(dsd.user_id, dst.user_id)                         AS user_id,
        dsd.account_id,
        dsd.aiblock_id                                           AS aiblock_id,
        is_template,
        DATE_TRUNC(dsd.date_datapoint_uploaded, DAY)             AS relevant_day,
        COUNT(dsd.datapoint_id)                                  AS net_data_points, 
        MAX(dsd.date_datapoint_uploaded)                         AS time_stamp,
    FROM datasets_data dsd
    INNER JOIN datasets_dataset dst ON dsd.aiblock_id = dst.aiblock_id
    WHERE DATE(TIMESTAMP_TRUNC(dsd.date_datapoint_uploaded, DAY)) = DATE_SUB(CURRENT_DATE(),INTERVAL 1 DAY)
    GROUP BY 
        1, 
        2, 
        3, 
        4,
        5

)

SELECT 
    final.user_id,
    final.account_id,
    sample_user,
    aiblock_id,
    is_template,
    net_data_points,
    relevant_day,
    time_stamp
FROM final
INNER JOIN onboarded_accounts ob ON final.account_id = ob.account_id


