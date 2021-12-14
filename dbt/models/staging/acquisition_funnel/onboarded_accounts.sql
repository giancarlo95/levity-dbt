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
    MIN(CASE WHEN is_deleted=1 THEN NULL ELSE user_email_address END) AS sample_user,
    MIN(date_user_onboarded)               AS date_account_onboarded,
    MAX(customer_status)                   AS customer_status,
    MAX(company_name)                      AS company_name
FROM onboarded_users
GROUP BY logged_account_id
