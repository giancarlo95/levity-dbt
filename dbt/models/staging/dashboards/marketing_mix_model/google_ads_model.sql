{{
  config(
    materialized = 'table',
    )
}}

SELECT 
    DATE(g.Date) AS relevant_date,
    SUM(g.interactions) AS g_interactions,
    SUM(g.impressions) AS g_impressions,
    COALESCE(CASE WHEN SUM(g.impressions)>0 THEN 1 ELSE 0 END, 0) AS g_dummy
FROM 
    {{ref('google_ads')}} g
GROUP BY 
    1
ORDER BY 
    1
    