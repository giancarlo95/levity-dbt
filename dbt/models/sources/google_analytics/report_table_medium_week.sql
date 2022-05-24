WITH source AS (

    SELECT * FROM {{ source('google_cloud_function_google_analytics_report', 'report_table_medium_week')}}

), renamed AS (

    SELECT
        medium,
        year_week,
        users
    FROM 
        source s

)

SELECT 
  year_week, 
  MAX(IF(medium = "organic", users, 0)) AS organic_new_website_visitors_count,
  MAX(IF(medium = "referral", users, 0)) AS referrals_new_website_visitors_count,
  MAX(IF(medium = "email", users, 0)) AS email_new_website_visitors_count,
  MAX(IF(medium = "social", users, 0)) AS social_new_website_visitors_count
FROM 
    renamed r 
GROUP BY 
    year_week
ORDER BY 
    year_week