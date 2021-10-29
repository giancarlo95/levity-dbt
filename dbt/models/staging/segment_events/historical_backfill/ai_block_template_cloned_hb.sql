WITH datasets_dataset AS (

    SELECT 
        aiblock_id,
        user_id,
        account_id,
        date_aiblock_created
    FROM 
        {{ref('datasets_dataset')}}
    WHERE 
        is_template="yes"

)

/* For ai_block_template_cloned we need the same info as ai_block_created + is_template field. */

SELECT
    *
FROM datasets_dataset
WHERE TIMESTAMP_DIFF(TIMESTAMP "2021-09-21 00:00:00+00", date_aiblock_created, HOUR)>0

