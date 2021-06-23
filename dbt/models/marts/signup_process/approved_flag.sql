WITH accounts_userprofile AS (

    SELECT * FROM {{ref('accounts_userprofile')}}
       
), auth_user AS (

    SELECT * FROM {{ref('auth_user')}}

)

SELECT 
    auth_user.user_email_address,
    accounts_userprofile.is_approved AS account_approved
FROM accounts_userprofile
LEFT JOIN auth_user ON 
    accounts_userprofile.user_id=auth_user.user_id
