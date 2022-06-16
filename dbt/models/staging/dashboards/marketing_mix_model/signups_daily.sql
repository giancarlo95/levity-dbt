{{
  config(
    materialized = 'table',
    )
}}

WITH signup_funnel AS (

    SELECT
       DATE(properties_sign_up_date_value) AS relevant_date,
        "yes" AS is_app_signup,
        COUNT(*) AS signups_count,
        COUNT(CASE WHEN properties_frontegg_email_confirmed_value="true" THEN 1 END) AS email_confirmed_count,
        COUNT(CASE WHEN properties_hs_lifecyclestage_marketingqualifiedlead_date_value IS NOT NULL THEN 1 END) AS mqls_count,
        COUNT(CASE WHEN properties_lead_score_value = '60' THEN 1 END) AS high_score_mqls_count,
        COUNT(CASE WHEN properties_hs_analytics_source_value = "ORGANIC_SEARCH" THEN 1 END) AS organic_signups_count,
        COUNT(CASE WHEN properties_frontegg_email_confirmed_value="true" AND properties_hs_analytics_source_value = "ORGANIC_SEARCH" THEN 1 END) AS organic_email_confirmed_count,
        COUNT(CASE WHEN properties_hs_lifecyclestage_marketingqualifiedlead_date_value IS NOT NULL AND properties_hs_analytics_source_value = "ORGANIC_SEARCH" THEN 1 END) AS organic_mqls_count,
        COUNT(CASE WHEN properties_lead_score_value = '60' AND properties_hs_analytics_source_value = "ORGANIC_SEARCH" THEN 1 END) AS organic_high_score_mqls_count,
        COUNT(CASE WHEN properties_hs_analytics_source_value = "PAID_SEARCH" THEN 1 END) AS paid_signups_count,
        COUNT(CASE WHEN properties_frontegg_email_confirmed_value="true" AND properties_hs_analytics_source_value = "PAID_SEARCH" THEN 1 END) AS paid_email_confirmed_count,
        COUNT(CASE WHEN properties_hs_lifecyclestage_marketingqualifiedlead_date_value IS NOT NULL AND properties_hs_analytics_source_value = "PAID_SEARCH" THEN 1 END) AS paid_mqls_count,
        COUNT(CASE WHEN properties_lead_score_value = '60' AND properties_hs_analytics_source_value = "PAID_SEARCH" THEN 1 END) AS paid_high_score_mqls_count,
        COUNT(CASE WHEN properties_hs_analytics_source_value = "PAID_SOCIAL" THEN 1 END) AS paid_social_signups_count,
        COUNT(CASE WHEN properties_frontegg_email_confirmed_value="true" AND properties_hs_analytics_source_value = "PAID_SOCIAL" THEN 1 END) AS paid_social_email_confirmed_count,
        COUNT(CASE WHEN properties_hs_lifecyclestage_marketingqualifiedlead_date_value IS NOT NULL AND properties_hs_analytics_source_value = "PAID_SOCIAL" THEN 1 END) AS paid_social_mqls_count,
        COUNT(CASE WHEN properties_lead_score_value = '60' AND properties_hs_analytics_source_value = "PAID_SOCIAL" THEN 1 END) AS paid_social_high_score_mqls_count,
    FROM 
        {{ref('hubspot_crm_contacts')}} 
    WHERE 
        NOT(email LIKE "%levity.ai")
        AND properties_retargeting_trial_status_value IS NULL
        AND DATE(properties_sign_up_date_value)>="2022-03-07"
    GROUP BY
        1

), legacy_signup_funnel_2022 AS (

    SELECT 
        f.submit_date AS relevant_date,
        "no" AS is_app_signup,
        COUNT(CASE WHEN f.is_company_email = "yes" THEN 1 END) AS signups_count,
        0 AS email_confirmed_count,
        COUNT(CASE WHEN f.is_company_email = "yes" AND is_qualified = "yes" THEN 1 END) AS mqls_count,
        COUNT(CASE WHEN f.is_company_email = "yes" AND is_high_score = 1 THEN 1 END) AS high_score_mqls_count,
        0 AS organic_signups_count,
        0 AS organic_email_confirmed_count,
        0 AS organic_mqls_count,
        0 AS organic_high_score_mqls_count,
        0 AS paid_signups_count,
        0 AS paid_email_confirmed_count,
        0 AS paid_mqls_count,
        0 AS paid_high_score_mqls_count,
        0 AS paid_social_signups_count,
        0 AS paid_social_email_confirmed_count,
        0 AS paid_social_mqls_count,
        0 AS paid_social_high_score_mqls_count
    FROM
        {{ref("first_typeform")}} f
    LEFT JOIN {{ref("second_typeform")}} s USING (email)
    LEFT JOIN {{ref("second_typeform_answers_all_time")}} sa ON sa.id=s.id
    WHERE 
        NOT(f.email LIKE "%levity.ai")
        AND f.submit_date<"2022-03-07"
    GROUP BY
        1

), legacy_signup_funnel_2021 AS (

    SELECT 
        f.submit_date AS relevant_date,
        "no" AS is_app_signup,
        COUNT(CASE WHEN f.is_company_email = "yes" THEN 1 END) AS signups_count,
        0 AS email_confirmed_count,
        COUNT(CASE WHEN f.is_company_email = "yes" AND is_qualified = "yes" THEN 1 END) AS mqls_count,
        COUNT(CASE WHEN f.is_company_email = "yes" AND is_high_score = 1 THEN 1 END) AS high_score_mqls_count,
        0 AS organic_signups_count,
        0 AS organic_email_confirmed_count,
        0 AS organic_mqls_count,
        0 AS organic_high_score_mqls_count,
        0 AS paid_signups_count,
        0 AS paid_email_confirmed_count,
        0 AS paid_mqls_count,
        0 AS paid_high_score_mqls_count,
        0 AS paid_social_signups_count,
        0 AS paid_social_email_confirmed_count,
        0 AS paid_social_mqls_count,
        0 AS paid_social_high_score_mqls_count
    FROM
        {{ref("first_typeform_2021")}} f
    LEFT JOIN {{ref("second_typeform_2021")}} s USING (email)
    LEFT JOIN {{ref("second_typeform_answers_all_time")}} sa ON sa.id=s.id
    WHERE 
        NOT(f.email LIKE "%levity.ai")
    GROUP BY
        1

)

SELECT * FROM legacy_signup_funnel_2021 UNION ALL
SELECT * FROM legacy_signup_funnel_2022 UNION ALL
SELECT * FROM signup_funnel
