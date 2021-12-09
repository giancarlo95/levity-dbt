WITH auth_user AS (

    SELECT
       user_id,
       username,
       user_email_address,
       date_user_onboarded 
    FROM 
       {{ref("auth_user")}}

), user_table AS (

    SELECT
       user_id,
       logged_account_id,
       user_name,
       user_email_address,
       date_user_onboarded,
       is_deleted
    FROM 
       {{ref("user_table")}}
    WHERE 
        is_locked=FALSE

),  user_invited AS (

    SELECT					
        DISTINCT user_id,			
    FROM 
       {{ref("user_invited")}}

), datasets_dataset AS (

    SELECT 
       DISTINCT user_id, old_user_id
    FROM 
       {{ref("datasets_dataset")}}
    WHERE 
        old_user_id IS NOT NULL
        AND NOT(old_user_id="286")

), users AS (

    SELECT 
       DISTINCT user_id, user_email_address
    FROM 
       {{ref("users")}}

), contact_enhanced AS (

    SELECT 
        contact_email,
        custom_status,
        all_contact_company_names
    FROM 
       {{ref('contact_enhanced')}}
    WHERE  
        internal_user=false
        AND contact_role="user"

), accounts_paymentplan AS (

    SELECT
        *
    FROM 
        (SELECT 
            account_id,
            ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY date_plan_updated DESC) AS index,
            plan_type,
            plan_status
        FROM 
           {{ref('accounts_paymentplan')}}) AS plans_all_times
    WHERE 
        index=1

), final AS (

    SELECT 
        user_table.user_id,
        user_table.logged_account_id,
        user_table.user_name,
        datasets_dataset.old_user_id,
        CASE 
            WHEN user_table.user_email_address IS NULL AND auth_user.user_email_address IS NULL THEN LOWER(users.user_email_address)
            WHEN user_table.user_email_address IS NULL AND NOT(auth_user.user_email_address IS NULL) THEN LOWER(auth_user.user_email_address)
            ELSE LOWER(user_table.user_email_address)
        END                                         AS user_email_address,
        CASE 
            WHEN auth_user.date_user_onboarded IS NULL THEN user_table.date_user_onboarded
            ELSE auth_user.date_user_onboarded
        END                                         AS date_user_onboarded,
        user_table.is_deleted
    FROM user_table
    LEFT JOIN 
        datasets_dataset ON datasets_dataset.user_id=user_table.user_id
    LEFT JOIN 
        auth_user ON auth_user.user_id=datasets_dataset.old_user_id
    LEFT JOIN 
        users ON users.user_id=user_table.user_id

) 

SELECT 
    final.user_id,
    final.logged_account_id,
    final.old_user_id,
    final.user_email_address,
    final.date_user_onboarded                                                                                   AS date_user_onboarded,
    final.is_deleted,
    CASE
        WHEN user_invited.user_id IS NULL THEN "Onboarded" 
        ELSE "Added"  
    END                                                                                                         AS flag,
    MAX(contact_enhanced.custom_status) OVER (PARTITION BY final.logged_account_id)                             AS customer_status,
    MAX(contact_enhanced.all_contact_company_names) OVER (PARTITION BY final.logged_account_id)                 AS company_name,
    accounts_paymentplan.plan_type,
    CASE 
        WHEN accounts_paymentplan.plan_status="active" THEN true
        ELSE false
    END                                                                                                         AS plan_status,           
FROM 
    final
INNER JOIN 
    contact_enhanced ON contact_enhanced.contact_email=final.user_email_address
LEFT JOIN 
    user_invited ON user_invited.user_id=final.user_id
LEFT JOIN 
    accounts_paymentplan ON accounts_paymentplan.account_id=final.logged_account_id