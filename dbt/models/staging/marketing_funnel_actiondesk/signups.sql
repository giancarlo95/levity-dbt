WITH contact_enhanced AS (

    SELECT 
        contact_id,
        contact_email,
        contact_role,
        custom_type_of_data,
        CASE 
            WHEN custom_lead_score>30 THEN "high score"
            WHEN custom_lead_score<=30 THEN "low score"
            WHEN custom_lead_score IS NULL THEN NULL
        END                                 AS score,
        all_contact_company_names,
        CASE 
            WHEN signed_up_at IS NULL THEN created_at
            ELSE signed_up_at 
        END                                 AS signed_up_at,
        CASE 
            WHEN all_contact_tags_fill LIKE "%Access Call booked%" THEN true
            ELSE false
        END                                 AS access_call_booked,
        CASE 
            WHEN all_contact_tags_fill LIKE "%Hanna pre-qualified%" THEN "pre-qualified"
            WHEN all_contact_tags_fill LIKE "%Gero Pre-qualified%" THEN "pre-qualified"
            ELSE "not pre-qualified"
        END                                 AS qualification
    FROM 
       {{ref('contact_enhanced')}}
    

), typeform_first_step AS (

    SELECT 
        DISTINCT  LOWER(email_address)       AS email_address,
        true                                 AS typeform
    FROM 
       {{ref('typeform_first_step')}}

), transformed AS (

    SELECT 
        contact_email,
        contact_role,
        custom_type_of_data,
        score,
        all_contact_company_names,
        access_call_booked,
        qualification,
        CAST(signed_up_at AS DATE)                             AS date_signed_up,
        LAST_DAY(CAST(signed_up_at AS DATE), WEEK)             AS week_end,
        EXTRACT(WEEK FROM CAST(signed_up_at AS DATE))          AS week,
        EXTRACT(YEAR FROM CAST(signed_up_at AS DATE))          AS year,
        DATE_TRUNC(CAST(signed_up_at AS DATE), MONTH)          AS month_begin,
        CONCAT(EXTRACT(YEAR FROM CAST(signed_up_at AS DATE)), EXTRACT(WEEK FROM CAST(signed_up_at AS DATE))) AS year_week 
    FROM 
        contact_enhanced

), onboarded_user_accounts AS (

    SELECT 
        user_email_address,
        user_id,
        account_id,
        CAST(date_user_onboarded AS DATE)   AS date_user_onboarded,
        flag
    FROM 
        {{ref('onboarded_user_accounts')}}

), final AS (

    SELECT 
        contact_email,
        all_contact_company_names,
        CASE 
            WHEN all_contact_company_names="Audibene" AND flag IS NOT NULL THEN MAX(account_id) OVER(PARTITION BY all_contact_company_names)
            ELSE account_id
        END                                                                         AS account_id,                                                                         
        CASE 
            WHEN typeform IS NULL OR DATE_DIFF(date_signed_up, "2021-06-07", DAY)<0 THEN false
            ELSE true
        END                                                                         AS typeform,
        contact_role,
        custom_type_of_data,
        score,
        date_signed_up,
        week_end,
        month_begin,
        access_call_booked,
        qualification,
        CASE 
            WHEN flag IS NULL THEN false
            ELSE true 
        END                                                                          AS onboarded,
        date_user_onboarded,
        user_id
    FROM 
        transformed
    LEFT JOIN typeform_first_step 
        ON typeform_first_step.email_address=transformed.contact_email
    LEFT JOIN onboarded_user_accounts 
        ON transformed.contact_email=onboarded_user_accounts.user_email_address
    WHERE 
        contact_email IS NOT NULL

)

SELECT 
    contact_email,
    all_contact_company_names,
    CASE 
        WHEN all_contact_company_names IS NOT NULL THEN ROW_NUMBER() OVER (PARTITION BY all_contact_company_names ORDER BY date_signed_up ASC) 
        ELSE NULL 
    END                                                                         AS signup_expansion,
    typeform,
    contact_role,
    custom_type_of_data,
    score,
    date_signed_up,
    week_end,
    month_begin,
    access_call_booked,
    qualification,
    onboarded,
    date_user_onboarded,
    account_id,
    CASE 
        WHEN account_id IS NOT NULL THEN ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY date_user_onboarded ASC) 
        ELSE NULL 
    END                                                                          AS onboarding_expansion,
    user_id
FROM  
    final