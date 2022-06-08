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
        1,
        2

), legacy_signup_funnel_2022 AS (

    SELECT 
        EXTRACT(YEAR FROM f.submit_date) AS year,
        EXTRACT(WEEK(MONDAY) FROM f.submit_date) AS week,
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
        1,
        2

), legacy_signup_funnel_2021 AS (

    SELECT 
        EXTRACT(YEAR FROM f.submit_date) AS year,
        EXTRACT(WEEK(MONDAY) FROM f.submit_date) AS week,
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
        1,
        2

), unioned_su AS (

    SELECT * FROM legacy_signup_funnel_2021 UNION ALL
    SELECT * FROM legacy_signup_funnel_2022 UNION ALL
    SELECT * FROM signup_funnel

), new_website_visitors_ga AS (

    SELECT 
        CASE WHEN year_week = 202201 THEN 2022 ELSE EXTRACT(YEAR FROM PARSE_DATE("%Y%W", CAST(year_week-1 AS STRING))) END AS year,
        CASE WHEN year_week = 202201 THEN 0 ELSE EXTRACT(WEEK(MONDAY) FROM PARSE_DATE("%Y%W", CAST(year_week-1 AS STRING))) END AS week,
        users AS new_website_visitors_count
    FROM 
        {{ref("report_table_week")}}
    WHERE
        PARSE_DATE("%Y%W", CAST(year_week-1 AS STRING))<"2022-03-07"

), new_website_visitors_hs AS (

    SELECT
        EXTRACT(YEAR FROM tw.date) AS year,
        EXTRACT(WEEK(MONDAY) FROM tw.date) AS week,
        visitors AS new_website_visitors_count
    FROM 
        {{ref("hubspot_analytics_total_weekly")}} tw
    WHERE
        tw.date>="2022-03-07"

), unioned_traffic AS (

    SELECT * FROM new_website_visitors_ga UNION ALL
    SELECT * FROM new_website_visitors_hs

), new_website_visitors_by_source_hs AS (

    SELECT
        EXTRACT(YEAR FROM sw.date) AS year,
        EXTRACT(WEEK(MONDAY) FROM sw.date) AS week,
        * EXCEPT(date)
    FROM 
        {{ref("hubspot_analytics_by_source_weekly")}} sw
    WHERE
        sw.date>="2022-03-07"

), new_website_visitors_by_source_ga AS (

    SELECT 
        CASE WHEN year_week = 202201 THEN 2022 ELSE EXTRACT(YEAR FROM PARSE_DATE("%Y%W", CAST(year_week-1 AS STRING))) END AS year,
        CASE WHEN year_week = 202201 THEN 0 ELSE EXTRACT(WEEK(MONDAY) FROM PARSE_DATE("%Y%W", CAST(year_week-1 AS STRING))) END AS week,
        * EXCEPT(year_week),
        0 AS paid_new_website_visitors_count,
        0 AS paid_social_new_website_visitors_count,
        0 AS direct_new_website_visitors_count,
        0 AS other_new_website_visitors_count
    FROM 
        {{ref("report_table_medium_week")}}
    WHERE
        PARSE_DATE("%Y%W", CAST(year_week-1 AS STRING))<"2022-03-07"

), unioned_traffic_by_source AS (

    SELECT * FROM new_website_visitors_by_source_ga UNION ALL
    SELECT * FROM new_website_visitors_by_source_hs

), users AS (

    SELECT 
        user_id,
        user_email_address AS email
    FROM
        {{ref("users")}} u

), user_onboarded AS (

    SELECT 
        user_id,
        MIN(DATE(uo.timestamp)) AS user_onboarded_at
    FROM
        {{ref("django_production_user_onboarded")}} uo
    GROUP BY
        user_id

), predictions_done AS (

    SELECT 
        user_id,
        MIN(DATE(uo.timestamp)) AS first_predictions_done_at
    FROM
        {{ref("django_production_predictions_done")}} uo
    GROUP BY
        user_id

), onboardings AS (

    SELECT 
        EXTRACT(YEAR FROM user_onboarded_at) AS year,
        EXTRACT(WEEK(MONDAY) FROM user_onboarded_at) AS week,
        COUNT(*) AS onboardings_count,
        COUNT(CASE WHEN first_predictions_done_at IS NOT NULL THEN 1 END) AS activations_count
    FROM
        user_onboarded uo
    LEFT JOIN users u USING(user_id)
    LEFT JOIN predictions_done USING(user_id)
    WHERE 
        NOT(u.email LIKE "%@levity.ai")
    GROUP BY
        1,
        2

)

SELECT
    PARSE_DATE("%Y%W", CAST(CONCAT(CAST(year AS STRING), CAST(week AS STRING)) AS STRING)) AS week_monday,
    * EXCEPT(onboardings_count, activations_count),
    COALESCE(onboardings_count, 0) AS onboardings_count,
    COALESCE(activations_count, 0) AS activations_count
FROM
    unioned_traffic 
JOIN unioned_traffic_by_source USING (year, week)
JOIN unioned_su USING (year, week)
LEFT JOIN onboardings USING (year, week)
ORDER BY
    year,
    week

