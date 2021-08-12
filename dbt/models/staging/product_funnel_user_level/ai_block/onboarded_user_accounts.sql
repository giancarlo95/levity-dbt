WITH onboarded_users AS (

    SELECT
       *
    FROM 
       {{ref('onboarded_users')}}

), user_table AS (

    SELECT
       *
    FROM 
       {{ref('user_table')}}

), datasets_dataset AS (

    SELECT
       DISTINCT user_id, account_id
    FROM 
       {{ref('datasets_dataset')}}    

)

SELECT 
    onboarded_users.user_id,
    onboarded_users.user_email_address,
    CASE 
        WHEN user_table.logged_account_id IS NULL THEN datasets_dataset.account_id
        ELSE user_table.logged_account_id 
    END                                                                             AS account_id,
    onboarded_users.date_user_onboarded,
    "Onboarded" AS flag
FROM onboarded_users 
    LEFT JOIN datasets_dataset ON onboarded_users.user_id=datasets_dataset.user_id
    LEFT JOIN user_table ON user_table.user_id=onboarded_users.user_id


