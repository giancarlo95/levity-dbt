{{
  config(
    materialized = 'table',
    )
}}

WITH facebook_ads_data AS (

    SELECT 
        DATE(fba.date) AS relevant_date,
        campaign_name
        impressions,
        clicks,
        spend

    FROM 
        {{ref('facebook_ads')}} fba
)

SELECT 
    relevant_date,

    MAX ( IF ( CONTAINS (campaign_name, 'Ebook'), impressions, 0 ) ) AS ebook_campaign_impressions,
    MAX ( IF ( CONTAINS (campaign_name, 'Ebook'), clicks, 0 ) ) AS ebook_campaign_clicks,
    MAX ( IF ( CONTAINS (campaign_name, 'Ebook'), spend, 0 ) ) AS ebook_campaign_spend,

    MAX ( IF ( CONTAINS (campaign_name, 'Retargeting'), impressions, 0 ) ) AS retargeting_campaign_impressions,
    MAX ( IF ( CONTAINS (campaign_name, 'Retargeting'), clicks, 0 ) ) AS retargeting_campaign_clicks,
    MAX ( IF ( CONTAINS (campaign_name, 'Retargeting'), spend, 0 ) ) AS retargeting_campaign_spend,

    MAX ( IF ( CONTAINS (campaign_name, 'Use'), impressions, 0 ) ) AS use_case_campaign_impressions,
    MAX ( IF ( CONTAINS (campaign_name, 'Use'), clicks, 0 ) ) AS use_case_campaign_clicks,
    MAX ( IF ( CONTAINS (campaign_name, 'Use'), spend, 0 ) ) AS use_case_campaign_spend,

    MAX ( IF ( CONTAINS (campaign_name, 'Lookalike'), impressions, 0 ) ) AS lookalike_campaign_impressions,
    MAX ( IF ( CONTAINS (campaign_name, 'Lookalike'), clicks, 0 ) ) AS lookalike_campaign_clicks,
    MAX ( IF ( CONTAINS (campaign_name, 'Lookalike'), spend, 0 ) ) AS lookalike_campaign_spend,

    MAX ( IF ( (CONTAINS (campaign_name, 'Main Conversion')) OR (CONTAINS (campaign_name, 'General Conversion')), impressions, 0 ) ) AS main_conversion_campaign_impressions,
    MAX ( IF ( (CONTAINS (campaign_name, 'Main Conversion')) OR (CONTAINS (campaign_name, 'General Conversion')), clicks, 0 ) ) AS main_conversion_campaign_clicks,
    MAX ( IF ( (CONTAINS (campaign_name, 'Main Conversion')) OR (CONTAINS (campaign_name, 'General Conversion')), spend, 0 ) ) AS main_conversion_campaign_spend,


FROM 
    facebook_ads_data 
GROUP BY 
    relevant_date
ORDER BY 
    relevant_date