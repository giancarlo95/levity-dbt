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

), onboarded_users AS (

    SELECT
        user_id,
        user_email_address
    FROM
        {{ref('onboarded_users')}}
        
)

/* For ai_block_created events we need dataset_id (aiblock_id) + user_id, account_id and email */

SELECT 
    aiblock_id,
    dts.user_id,
    account_id,
    date_aiblock_created,
    user_email_address
FROM datasets_dataset dts
INNER JOIN onboarded_users ob ON dts.user_id = ob.user_id
WHERE TIMESTAMP_DIFF(TIMESTAMP "2021-09-21 00:00:00+00", date_aiblock_created, HOUR)>0