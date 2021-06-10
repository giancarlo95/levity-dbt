WITH auth_user AS (

    SELECT
       user_id,
       user_email_address,
       date_user_onboarded 
    FROM 
       {{ref('auth_user')}}
    WHERE
       is_staff=FALSE
       AND NOT(user_email_address LIKE '%@angularventures.com' OR user_email_address LIKE '%@discovery-ventures.com' OR user_email_address LIKE '%@levity.ai')
       AND NOT(user_email_address IN ('abcaisodjaiosdjioasd@gmail.com', 'adil.islam619@gmail.com', 'milanjose999@gmail.com', 'hanna.kleinings@gmail.com'))

), accounts_userprofile AS (

    SELECT 
       user_id
    FROM 
       {{ref('accounts_userprofile')}}
    WHERE 
       is_approved=TRUE
)

SELECT 
    auth_user.user_id,
    user_email_address,
    date_user_onboarded
FROM auth_user 
INNER JOIN accounts_userprofile ON 
    auth_user.user_id=accounts_userprofile.user_id 
