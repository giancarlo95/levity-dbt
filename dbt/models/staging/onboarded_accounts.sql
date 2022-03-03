WITH onboarded_users AS (

    SELECT
       *
    FROM 
       {{ref('onboarded_users')}}

)

SELECT 
    logged_account_id,
    -- to avoid remaking all the charts in the dashboard
    logged_account_id                      AS account_id,
    -- 
    MIN(user_email_address)                AS sample_user,
    MIN(date_user_onboarded)               AS date_account_onboarded,
    MAX(company_name)                      AS company_name
FROM onboarded_users
GROUP BY logged_account_id
