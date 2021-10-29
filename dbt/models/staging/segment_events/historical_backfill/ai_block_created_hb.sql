WITH datasets_dataset AS (

    SELECT 
        aiblock_id,
        user_id,
        account_id
    FROM 
        {{ref('datasets_dataset')}}
    WHERE 
        is_template="no"

)

/* For ai_block_created events we need dataset_id (aiblock_id) + user_id, account_id and email */

SELECT 
    *
FROM datasets_dataset