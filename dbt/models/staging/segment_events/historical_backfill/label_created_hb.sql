WITH datasets_label AS (

    SELECT 
        *
    FROM 
        {{ref('datasets_label')}}

), onboarded_users AS (

    SELECT
        user_id,
        user_email_address
    FROM
        {{ref('onboarded_users')}}
        
), label_created AS (

    SELECT 
    
    FROM datasets_label
    I
)