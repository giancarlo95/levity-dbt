WITH datasets_data AS (

    SELECT 
        aiblock_id,
        date_aiblock_created,
        user_id
    FROM 
        {{ref('datasets_data')}}

), onboarded_users AS (

    SELECT
        user_id,
        user_email_address
    FROM
        {{ref('onboarded_users')}}

), final AS (

    SELECT 
        *
    FROM datasets_data dsd
    INNER JOIN onboarded_users obu ON dsd.user_id = obu.user_id

)