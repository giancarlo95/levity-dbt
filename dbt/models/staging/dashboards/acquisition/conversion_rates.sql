{{
  config(
    materialized = 'table',
    )
}}

WITH signup_funnel AS (

    SELECT
        EXTRACT(YEAR FROM properties_sign_up_date_value) AS year,
        EXTRACT(WEEK(MONDAY) FROM properties_sign_up_date_value) AS week,
        "yes" AS is_app_signup,
        COUNT(*) AS signups_count,
        COUNT(CASE WHEN properties_frontegg_email_confirmed_value="true" THEN 1 END) AS email_confirmed_count,
        COUNT(CASE WHEN properties_hs_lifecyclestage_marketingqualifiedlead_date_value IS NOT NULL THEN 1 END) AS mqls_count,
        COUNT(CASE WHEN properties_lead_score_value = '60' THEN 1 END) AS high_score_mqls_count,
        COUNT(CASE WHEN properties_hs_analytics_source_value = "ORGANIC_SEARCH" THEN 1 END) AS organic_signups_count,
        COUNT(CASE WHEN properties_frontegg_email_confirmed_value="true" AND properties_hs_analytics_source_value = "ORGANIC_SEARCH" THEN 1 END) AS organic_email_confirmed_count,
        COUNT(CASE WHEN properties_hs_lifecyclestage_marketingqualifiedlead_date_value IS NOT NULL AND properties_hs_analytics_source_value = "ORGANIC_SEARCH" THEN 1 END) AS organic_mqls_count,
        COUNT(CASE WHEN properties_lead_score_value = '60' AND properties_hs_analytics_source_value = "ORGANIC_SEARCH" THEN 1 END) AS organic_high_score_mqls_count,
        COUNT(CASE WHEN properties_hs_analytics_source_value = "PAID_SEARCH" THEN 1 END) AS paid_search_signups_count,
        COUNT(CASE WHEN properties_frontegg_email_confirmed_value="true" AND properties_hs_analytics_source_value = "PAID_SEARCH" THEN 1 END) AS paid_search_email_confirmed_count,
        COUNT(CASE WHEN properties_hs_lifecyclestage_marketingqualifiedlead_date_value IS NOT NULL AND properties_hs_analytics_source_value = "PAID_SEARCH" THEN 1 END) AS paid_search_mqls_count,
        COUNT(CASE WHEN properties_lead_score_value = '60' AND properties_hs_analytics_source_value = "PAID_SEARCH" THEN 1 END) AS paid_search_high_score_mqls_count,
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
        1,
        2

), legacy_signup_funnel AS (

    SELECT 
        EXTRACT(YEAR FROM f.submit_date) AS year,
        EXTRACT(WEEK(MONDAY) FROM f.submit_date) AS week,
        "no" AS is_app_signup,
        COUNT(CASE WHEN f.is_company_email = "yes" THEN 1 END) AS signups_count,
        0 AS email_confirmed_count,
        COUNT(CASE WHEN f.is_company_email = "yes" AND is_qualified = "yes" THEN 1 END) AS mqls_count,
        0 AS high_score_mqls_count,
        0 AS organic_signups_count,
        0 AS organic_email_confirmed_count,
        0 AS organic_mqls_count,
        0 AS organic_high_score_mqls_count,
        0 AS paid_search_signups_count,
        0 AS paid_search_email_confirmed_count,
        0 AS paid_search_mqls_count,
        0 AS paid_search_high_score_mqls_count,
        0 AS paid_social_signups_count,
        0 AS paid_social_email_confirmed_count,
        0 AS paid_social_mqls_count,
        0 AS paid_social_high_score_mqls_count
    FROM
        {{ref("first_typeform")}} f
    LEFT JOIN {{ref("second_typeform")}} s USING (email)
    WHERE 
        NOT(email LIKE "%levity.ai")
        AND f.submit_date<"2022-03-07"
    GROUP BY
        1,
        2

), unioned AS (

    SELECT * FROM signup_funnel UNION ALL
    SELECT * FROM legacy_signup_funnel

), new_website_visitors AS (

    SELECT 
        CASE WHEN year_week = 202201 THEN 2022 ELSE EXTRACT(YEAR FROM PARSE_DATE("%Y%W", CAST(year_week-1 AS STRING))) END AS year,
        CASE WHEN year_week = 202201 THEN 0 ELSE EXTRACT(WEEK(MONDAY) FROM PARSE_DATE("%Y%W", CAST(year_week-1 AS STRING))) END AS week,
        users AS new_website_visitors_count
    FROM 
        {{ref("report_table_week")}}

) 

SELECT
    *
FROM
    new_website_visitors 
JOIN unioned USING (year, week)
ORDER BY
    year,
    week

