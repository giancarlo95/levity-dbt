WITH datasets_label AS (

    SELECT 
        label_id,
        label_name,
        aiblock_id,
        user_id,
        account_id
    FROM 
        {{ref('datasets_label')}}

)

SELECT *
FROM datasets_label