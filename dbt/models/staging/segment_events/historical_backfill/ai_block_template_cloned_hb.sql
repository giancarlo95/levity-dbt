WITH datasets_dataset AS (

    SELECT 
        aiblock_id,
        is_template,
        user_id,
        account_id
    FROM 
        {{ref('datasets_dataset')}}

)

/* For ai_block_template_cloned we need the same info as ai_block_created + is_template field. */

SELECT
    *
FROM datasets_dataset

