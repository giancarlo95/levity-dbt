WITH source AS (

    SELECT * FROM {{ source('hubspot_traffic_analytics', 'hubspot_analytics_by_source_weekly')}}

), renamed AS (

    SELECT
        s.date,
        source,
        visitors
    FROM 
        source s

)

SELECT 
  r.date, 
  MAX(IF(source = "organic", visitors, 0)) AS organic_new_website_visitors_count,
  MAX(IF(source = "paid", visitors, 0)) AS paid_new_website_visitors_count,
  MAX(IF(source = "paid-social", visitors, 0)) AS paid_social_new_website_visitors_count,
  MAX(IF(source = "direct", visitors, 0)) AS direct_new_website_visitors_count,
  MAX(IF(source = "referrals", visitors, 0)) AS referrals_new_website_visitors_count,
  MAX(IF(source = "social", visitors, 0)) AS social_new_website_visitors_count,
  MAX(IF(source = "other", visitors, 0)) AS other_new_website_visitors_count,
  MAX(IF(source = "email", visitors, 0)) AS email_new_website_visitors_count
FROM 
    renamed r 
GROUP BY 
    r.date 
ORDER BY 
    r.date