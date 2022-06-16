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
    COALESCE(signups_count, 0) AS signups_count,
    CASE 
        WHEN relevant_date>="2022-03-07" THEN COALESCE(is_app_signup, 1)
        ELSE COALESCE(is_app_signup, 0)
    END AS is_app_signup,
    no_code_ai_map_clicks_count,
    no_code_ai_map_impressions_count,
    homepage_clicks_count,
    homepage_impressions_count,
    difference_ml_dl_clicks_count,
    difference_ml_dl_impressions_count,
FROM
    gsc_top_pages 
LEFT JOIN signups_daily USING(relevant_date)
ORDER BY
    relevant_date DESC


