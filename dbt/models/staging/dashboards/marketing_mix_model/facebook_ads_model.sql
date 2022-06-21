{{
  config(
    materialized = 'table',
    )
}}

WITH facebook_ads_data AS (

    SELECT 
        DATE(fba.date) AS relevant_date,
        campaign_name,
        impressions,
        clicks,
        spend

    FROM 
        {{ref('facebook_ads')}} fba
)

SELECT 
    relevant_date,

    SUM ( IF (campaign_name LIKE 'Ebook%', impressions, 0 ) ) AS ebook_campaign_impressions,
    SUM ( IF (campaign_name LIKE 'Ebook%', clicks, 0 ) ) AS ebook_campaign_clicks,
    SUM ( IF (campaign_name LIKE 'Ebook%', spend, 0 ) ) AS ebook_campaign_spend,

    SUM ( IF (campaign_name LIKE 'Re-Targeting%', impressions, 0 ) ) AS retargeting_campaign_impressions,
    SUM ( IF (campaign_name LIKE 'Re-Targeting%', clicks, 0 ) ) AS retargeting_campaign_clicks,
    SUM ( IF (campaign_name LIKE 'Re-Targeting%', spend, 0 ) ) AS retargeting_campaign_spend,

    SUM ( IF (campaign_name LIKE 'Use%', impressions, 0 ) ) AS use_case_campaign_impressions,
    SUM ( IF (campaign_name LIKE 'Use%', clicks, 0 ) ) AS use_case_campaign_clicks,
    SUM ( IF (campaign_name LIKE 'Use%', spend, 0 ) ) AS use_case_campaign_spend,

    SUM ( IF (campaign_name LIKE "Lookalike%", impressions, 0 ) ) AS lookalike_campaign_impressions,
    SUM ( IF (campaign_name LIKE "Lookalike%", clicks, 0 ) ) AS lookalike_campaign_clicks,
    SUM ( IF (campaign_name LIKE "Lookalike%", spend, 0 ) ) AS lookalike_campaign_spend,

    SUM ( IF (campaign_name LIKE 'Main Conversion%' OR campaign_name LIKE 'General Conversion%', impressions, 0 ) ) AS main_conversion_campaign_impressions,
    SUM ( IF (campaign_name LIKE 'Main Conversion%' OR campaign_name LIKE 'General Conversion%', clicks, 0 ) ) AS main_conversion_campaign_clicks,
    SUM ( IF (campaign_name LIKE 'Main Conversion%' OR campaign_name LIKE 'General Conversion%', spend, 0 ) ) AS main_conversion_campaign_spend


FROM 
    facebook_ads_data 
GROUP BY 
    relevant_date
ORDER BY 
    relevant_date