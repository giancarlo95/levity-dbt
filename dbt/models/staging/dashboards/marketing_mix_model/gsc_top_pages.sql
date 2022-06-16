{{
  config(
    materialized = 'table',
    )
}}

WITH clicks_impressions_by_page AS (

    SELECT 
        DATE(cibp.date) AS relevant_date,
        cibp.page AS landing_page,
        clicks,
        impressions
    FROM 
        {{ref('clicks_impressions_by_page')}} cibp
)

SELECT 
    relevant_date, 
    MAX(IF(landing_page = "https://levity.ai/blog/no-code-ai-map", clicks, 0)) AS no_code_ai_map_clicks_count,
    MAX(IF(landing_page = "https://levity.ai/", clicks, 0)) AS homepage_clicks_count,
    MAX(IF(landing_page = "https://levity.ai/blog/no-code-ai-map", impressions, 0)) AS no_code_ai_map_impressions_count,
    MAX(IF(landing_page = "https://levity.ai/", impressions, 0)) AS homepage_impressions_count,
    MAX(IF(landing_page = "https://levity.ai/blog/difference-machine-learning-deep-learning", impressions, 0)) AS difference_ml_dl_impressions_count,
    MAX(IF(landing_page = "https://levity.ai/blog/difference-machine-learning-deep-learning", clicks, 0)) AS difference_ml_dl_clicks_count,
FROM 
    clicks_impressions_by_page 
GROUP BY 
    relevant_date
ORDER BY 
    relevant_date