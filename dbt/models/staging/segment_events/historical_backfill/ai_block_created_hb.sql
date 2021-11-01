WITH datasets_dataset AS (

    SELECT 
        aiblock_id,
        user_id,
        account_id, 
        date_aiblock_created
    FROM 
        {{ref('datasets_dataset')}}
    WHERE 
        is_template="no"
        AND TIMESTAMP_DIFF(TIMESTAMP "2021-09-21 00:00:00+00", date_aiblock_created, HOUR)>0

)

/* For ai_block_created events we need dataset_id (aiblock_id) + user_id, account_id and email */

SELECT 
    *
FROM datasets_dataset