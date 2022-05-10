{{
  config(
    materialized = 'table',
    )
}}

WITH engagement AS (

    SELECT 
        email,
        DATE_DIFF(CURRENT_DATE(), DATE(properties_notes_last_contacted_value), DAY) AS days_since_last_contacted,
        DATE_DIFF(CURRENT_DATE(), DATE(properties_hs_sales_email_last_replied_value), DAY) AS days_since_last_heard_from,
        CASE
            WHEN DATE_DIFF(CURRENT_DATE(), DATE(properties_notes_last_contacted_value), DAY) <= 30 THEN 'green'
            WHEN DATE_DIFF(CURRENT_DATE(), DATE(properties_notes_last_contacted_value), DAY) BETWEEN 30 and 60 THEN 'yellow' 
            WHEN DATE_DIFF(CURRENT_DATE(), DATE(properties_notes_last_contacted_value), DAY) > 60 THEN 'red'
            ELSE NULL 
        END AS days_since_last_contacted_discrete,
        CASE 
            WHEN DATE_DIFF(CURRENT_DATE(), DATE(properties_hs_sales_email_last_replied_value), DAY) <= 30 THEN 'green'
            WHEN DATE_DIFF(CURRENT_DATE(), DATE(properties_hs_sales_email_last_replied_value), DAY) BETWEEN 30 and 60 THEN 'yellow'
            WHEN DATE_DIFF(CURRENT_DATE(), DATE(properties_hs_sales_email_last_replied_value), DAY) > 60 THEN 'red'
            ELSE NULL
        END AS days_since_last_heard_from_discrete
    FROM 
        {{ref('hubspot_crm_contacts')}}

),

login_last7 AS (

    SELECT 
        user_id,
        DATE_DIFF(CURRENT_DATE(), DATE(MAX(original_timestamp)), DAY) AS days_since_last_login,
        CASE
            WHEN DATE_DIFF(CURRENT_DATE(), DATE(MAX(original_timestamp)), DAY) < 7 THEN 'green'
            ELSE 'red' 
        END AS user_logged_in_last7
    FROM 
        {{ref('django_production_user_signed_in')}}
    GROUP BY 
        user_id

),

actions_last30 AS (

    SELECT
        user_id,
        COUNT(id) AS actions_count_last30,
        CASE
            WHEN COUNT(id) >= 50 THEN 'green'
            ELSE 'red' 
        END AS at_least_50_actions_last30
    FROM 
        {{ref('django_production_actions')}}
    WHERE
        DATE_DIFF(CURRENT_DATE(), DATE(original_timestamp), DAY) BETWEEN 0 AND 30 
    GROUP BY 
        user_id

),

actions_last7 AS (

    SELECT
        user_id,
        COUNT(id) AS actions_count_last7,
        CASE
            WHEN COUNT(id) >= 50 THEN 'green'
            ELSE 'red' 
        END AS at_least_50_actions_last7
    FROM 
        {{ref('django_production_actions')}}
    WHERE
        DATE_DIFF(CURRENT_DATE(), DATE(original_timestamp), DAY) BETWEEN 0 AND 7 
    GROUP BY 
        user_id

),

ai_block_trained_last30 AS (

    SELECT
        user_id,
        COUNT(id) AS ai_blocks_trained_count_last30,
        CASE 
            WHEN COUNT(id) >= 1 THEN 'green'
            ELSE 'red' 
        END AS at_least_1_ai_block_trained_last30  
    FROM 
        {{ref('django_production_ai_block_trained')}}
    WHERE
        DATE_DIFF(CURRENT_DATE(), DATE(original_timestamp), DAY) BETWEEN 0 AND 30
    GROUP BY 
        user_id 

), 

ai_template_used_last30 AS (

    SELECT
        user_id,
        COUNT(id) AS ai_templates_used_count_last30,
        CASE 
            WHEN COUNT(id) >= 1 THEN 'green'
            ELSE 'red' 
        END AS at_least_1_ai_template_used_last30
    FROM 
        {{ref('django_production_ai_block_template')}}
    WHERE
        DATE_DIFF(CURRENT_DATE(), DATE(original_timestamp), DAY) BETWEEN 0 AND 30
    GROUP BY 
        user_id

),

days_since_onboarding AS (

    SELECT
        user_id,
        DATE_DIFF(CURRENT_DATE(), DATE(original_timestamp), DAY) AS days_since_onboarded,
        CASE
            WHEN DATE_DIFF(CURRENT_DATE(), DATE(original_timestamp), DAY) > 180 THEN 'green'
            WHEN DATE_DIFF(CURRENT_DATE(), DATE(original_timestamp), DAY) BETWEEN 90 AND 180 THEN 'yellow'
            WHEN DATE_DIFF(CURRENT_DATE(), DATE(original_timestamp), DAY) < 90 THEN 'red'
            ELSE NULL
        END AS days_since_onboarded_discrete
    FROM 
        {{ref('django_production_user_onboarded')}}

),

users AS (

    SELECT 
        *
    FROM
        {{ref('users')}}

)

SELECT
    e.email,
    e.days_since_last_contacted,
    e.days_since_last_contacted_discrete,
    e.days_since_last_heard_from,
    e.days_since_last_heard_from_discrete,
    l.days_since_last_login,
    l.user_logged_in_last7,
    a30.actions_count_last30,
    a30.at_least_50_actions_last30,
    a7.actions_count_last7,
    a7.at_least_50_actions_last7,
    tr.ai_blocks_trained_count_last30,
    tr.at_least_1_ai_block_trained_last30,
    te.ai_templates_used_count_last30,
    te.at_least_1_ai_template_used_last30,
    o.days_since_onboarded,
    o.days_since_onboarded_discrete
FROM engagement e
LEFT JOIN users u ON u.user_email_address=e.email
LEFT JOIN login_last7 l ON l.user_id=u.user_id
LEFT JOIN actions_last30 a30 ON a30.user_id=u.user_id
LEFT JOIN actions_last7 a7 ON a7.user_id=u.user_id
LEFT JOIN ai_block_trained_last30 tr ON tr.user_id=u.user_id
LEFT JOIN ai_template_used_last30 te ON te.user_id=u.user_id
LEFT JOIN days_since_onboarding o ON o.user_id=u.user_id
WHERE
    e.email NOT LIKE '%@levity.ai'