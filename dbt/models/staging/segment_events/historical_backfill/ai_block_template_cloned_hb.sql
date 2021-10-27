WITH datasets_data AS (

    SELECT 
        *
    FROM 
        {{ref('datasets_data')}}

), onboarded_users AS (

    SELECT
        user_id,
        user_email_address
    FROM
        {{ref('onboarded_users')}}
        
)