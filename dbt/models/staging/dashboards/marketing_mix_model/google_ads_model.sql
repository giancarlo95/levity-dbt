{{
  config(
    materialized = 'table',
    )
}}


WITH google_ads_data AS (

    SELECT 
        DATE(g.Date) AS relevant_date,
        SUM(g.interactions) AS g_interactions,
        SUM(g.impressions) AS g_impressions
    FROM 
        {{ref('google_ads')}} g
    GROUP BY 1
    ORDER BY 1
    
)