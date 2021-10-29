WITH datasets_dataset AS (

    SELECT 
        aiblock_id,
        user_id,
        account_id
    FROM 
        {{ref('datasets_dataset')}}
    WHERE 
        is_template="yes"

)

/* For ai_block_template_cloned we need the same info as ai_block_created + is_template field. */

SELECT
    *
FROM datasets_dataset

