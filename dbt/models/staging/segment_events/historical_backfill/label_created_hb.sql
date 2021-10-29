WITH datasets_label AS (

    SELECT 
        label_id,
        label_name,
        aiblock_id,
        user_id,
        account_id
    FROM 
        {{ref('datasets_label')}}

), onboarded_users AS (

    SELECT
        user_id,
        user_email_address
    FROM
        {{ref('onboarded_users')}}
        
)

SELECT *
FROM datasets_label dl
INNER JOIN onboarded_users ob ON dl.user_id = ob.user_id