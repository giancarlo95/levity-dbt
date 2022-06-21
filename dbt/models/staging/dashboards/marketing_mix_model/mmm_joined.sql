{{
  config(
    materialized = 'table',
    )
}}

WITH gsc_top_pages AS (

    SELECT 
        *
    FROM 
        {{ref('gsc_top_pages')}}

), 

facebook_ads_model AS (

    SELECT 
        *
    FROM
        {{ref('facebook_ads_model')}}
),

google_ads_model AS (

    SELECT
        *
    FROM 
        {{ref('google_ads_model')}}

),

signups_daily AS (

    SELECT 
        * EXCEPT(is_app_signup),
        CASE 
            WHEN is_app_signup = "yes" THEN 1
            ELSE 0
        END AS is_app_signup
    FROM 
        {{ref('signups_daily')}}

)

SELECT 
    * EXCEPT(signups_count, is_app_signup, g_impressions, g_interactions, g_dummy),
    COALESCE(signups_count, 0) AS signups_count,
    CASE 
        WHEN relevant_date>="2022-03-07" THEN COALESCE(is_app_signup, 1)
        ELSE COALESCE(is_app_signup, 0)
    END AS is_app_signup,
    COALESCE(g_impressions, 0) AS g_impressions,
    COALESCE(g_interactions, 0) AS g_interactions,
    COALESCE(g_dummy, 0) AS g_dummy
FROM
    gsc_top_pages gtp

LEFT JOIN signups_daily USING(relevant_date)
LEFT JOIN facebook_ads_model USING(relevant_date)
LEFT JOIN google_ads_model USING(relevant_date)

WHERE
    relevant_date>="2021-06-15"

ORDER BY
    relevant_date DESC


