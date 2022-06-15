{# {{
  config(
    materialized = 'table',
    )
}}

WITH engagement AS (

    SELECT 
        LOWER(email) AS email,
        DATE_DIFF(CURRENT_DATE(), DATE(properties_hs_last_sales_activity_timestamp_value), DAY) AS days_since_last_engagement,
        DATE_DIFF(CURRENT_DATE(), DATE(GREATEST(COALESCE(properties_hs_sales_email_last_replied_value, properties_hs_email_last_reply_date_value), COALESCE(properties_hs_email_last_reply_date_value, properties_hs_sales_email_last_replied_value))), DAY) AS days_since_last_heard_from,
        CASE
            WHEN DATE_DIFF(CURRENT_DATE(), DATE(properties_hs_last_sales_activity_timestamp_value), DAY) <= 30 THEN 'green'
            WHEN DATE_DIFF(CURRENT_DATE(), DATE(properties_hs_last_sales_activity_timestamp_value), DAY) BETWEEN 30 and 60 THEN 'yellow' 
            WHEN DATE_DIFF(CURRENT_DATE(), DATE(properties_hs_last_sales_activity_timestamp_value), DAY) > 60 THEN 'red'
            ELSE NULL 
        END AS days_since_last_engagement_discrete
    FROM 
        {{ref('hubspot_crm_contacts')}}
    WHERE   
        properties_is_onboarded_value = "yes"

),

login_last7 AS (

    SELECT 
        s.user_id,
        DATE_DIFF(CURRENT_DATE(), DATE(MAX(s.original_timestamp)), DAY) AS days_since_last_login,
        CASE
            WHEN DATE_DIFF(CURRENT_DATE(), DATE(MAX(s.original_timestamp)), DAY) < 7 THEN 'green'
            ELSE 'red' 
        END AS user_logged_in_last7
    FROM 
        {{ref('django_production_actions')}} s
    LEFT JOIN 
        {{ref('django_production_datapoints_added_view')}} d USING(id)
    WHERE 
        NOT(s.event IN ("predictions_done", "user_signed_up", "email_confirmed", "typeform_filled", "user_onboarded"))
        AND NOT(s.event = "datapoints_added" AND d.is_human_in_the_loop = "yes")
    GROUP BY 
        s.user_id

) #}