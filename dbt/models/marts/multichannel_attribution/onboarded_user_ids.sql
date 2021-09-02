WITH onboarded_users AS (

    SELECT 
        *
    FROM 
       {{ref('onboarded_users')}} 
    

), auth_user AS (

    SELECT 
        user_id,
        user_email_address
    FROM
        {{ref('auth_user')}}

) 

SELECT 
    onboarded_users.date_user_onboarded,
    onboarded_users.user_email_address,
    onboarded_users.user_id,
    auth_user.user_id                              AS old_user_id
FROM 
    onboarded_users 
LEFT JOIN auth_user
    ON auth_user.user_email_address=onboarded_users.user_email_address
