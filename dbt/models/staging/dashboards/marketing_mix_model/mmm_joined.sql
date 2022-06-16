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
    relevant_date,
    COALESCE(sud.signups_count, 0) AS signups_count,
    CASE 
        WHEN relevant_date>="2022-03-07" THEN COALESCE(sud.is_app_signup, 1)
        ELSE COALESCE(sud.is_app_signup, 0)
    END AS is_app_signup,
    gtp.no_code_ai_map_clicks_count,
    gtp.no_code_ai_map_impressions_count,
    gtp.homepage_clicks_count,
    gtp.homepage_impressions_count,
    gtp.difference_ml_dl_clicks_count,
    gtp.difference_ml_dl_impressions_count,

    fam.ebook_campaign_impressions,
    fam.ebook_campaign_clicks,
    fam.ebook_campaign_spend,

    fam.retargeting_campaign_impressions,
    fam.retargeting_campaign_clicks,
    fam.retargeting_campaign_spend,

    fam.lookalike_campaign_impressions,
    fam.lookalike_campaign_clicks,
    fam.lookalike_campaign_spend,

    fam.use_case_campaign_impressions,
    fam.use_case_campaign_clicks,
    fam.use_case_campaign_spend,

    fam.main_conversion_campaign_impressions,
    fam.main_conversion_campaign_clicks,
    fam.main_conversion_campaign_spend

    gam.g_impressions,
    gam.g_interactions

FROM
    gsc_top_pages gtp

LEFT JOIN signups_daily USING(relevant_date) sud
RIGHT JOIN facebook_ads_model USING(relevant_date) fam
RIGHT JOIN google_ads_model USING(relevant_date) gam

ORDER BY
    relevant_date DESC


