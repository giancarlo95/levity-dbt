{{
  config(
    materialized = 'table',
    )
}}

WITH engagement AS (

    SELECT 
        email,
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
        user_id,
        DATE_DIFF(CURRENT_DATE(), DATE(MAX(s.original_timestamp)), DAY) AS days_since_last_login,
        CASE
            WHEN DATE_DIFF(CURRENT_DATE(), DATE(MAX(s.original_timestamp)), DAY) < 7 THEN 'green'
            ELSE 'red' 
        END AS user_logged_in_last7
    FROM 
        {{ref('django_production_user_signed_in')}} s
    GROUP BY 
        user_id

),

actions_last30 AS (

    SELECT
        a.user_id,
        COUNT(a.id) AS actions_count_last30,
        CASE
            WHEN COUNT(a.id) >= 50 THEN 'green'
            ELSE 'red' 
        END AS at_least_50_actions_last30
    FROM 
        {{ref('django_production_actions')}} a
    WHERE
        NOT(a.event IN ("label_created", "label_deleted", "user_signed_up", "email_confirmed", "typeform_filled"))
        AND DATE_DIFF(CURRENT_DATE(), DATE(a.original_timestamp), DAY) BETWEEN 0 AND 30 
    GROUP BY 
        a.user_id

),

actions_last7 AS (

    SELECT
        a.user_id,
        COUNT(a.id) AS actions_count_last7,
        CASE
            WHEN COUNT(a.id) >= 50 THEN 'green'
            ELSE 'red' 
        END AS at_least_50_actions_last7
    FROM 
        {{ref('django_production_actions')}} a
    WHERE 
        NOT(a.event IN ("label_created", "label_deleted", "user_signed_up", "email_confirmed", "typeform_filled"))
        AND DATE_DIFF(CURRENT_DATE(), DATE(a.original_timestamp), DAY) BETWEEN 0 AND 7
    GROUP BY 
        a.user_id

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

milestone_ai_block_trained AS (

    SELECT
        user_id,
        COUNT(id) AS count_ai_blocks_trained,
        CASE 
            WHEN COUNT(id) >= 1 THEN 'green'
            ELSE 'red' 
        END AS milestone_1_ai_block_trained 
    FROM 
        {{ref('django_production_ai_block_trained')}}
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

),

joined_tables AS (

    SELECT
    e.email,
    e.days_since_last_engagement,
    e.days_since_last_engagement_discrete,
    e.days_since_last_heard_from,
    CASE 
        WHEN e.days_since_last_heard_from <= 30 THEN 'green'
        WHEN e.days_since_last_heard_from BETWEEN 30 and 60 THEN 'yellow'
        WHEN e.days_since_last_heard_from > 60 THEN 'red'
        ELSE NULL
    END AS days_since_last_heard_from_discrete,
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
    o.days_since_onboarded_discrete,
    mtr.milestone_1_ai_block_trained
    FROM engagement e
    LEFT JOIN users u ON u.user_email_address=e.email
    LEFT JOIN login_last7 l ON l.user_id=u.user_id
    LEFT JOIN actions_last30 a30 ON a30.user_id=u.user_id
    LEFT JOIN actions_last7 a7 ON a7.user_id=u.user_id
    LEFT JOIN ai_block_trained_last30 tr ON tr.user_id=u.user_id
    LEFT JOIN milestone_ai_block_trained mtr ON mtr.user_id=u.user_id
    LEFT JOIN ai_template_used_last30 te ON te.user_id=u.user_id
    LEFT JOIN days_since_onboarding o ON o.user_id=u.user_id
    WHERE
        e.email NOT LIKE '%@levity.ai'

),

score AS (

    SELECT email,

    ((CASE 
        WHEN days_since_last_engagement_discrete = 'green' THEN 1
        WHEN days_since_last_engagement_discrete = 'yellow' THEN 0
        WHEN days_since_last_engagement_discrete = 'red' THEN -1
    ELSE 0 END) * 0.75

    + 

    (CASE 
        WHEN days_since_last_heard_from_discrete = 'green' THEN 1
        WHEN days_since_last_heard_from_discrete = 'yellow' THEN 0
        WHEN days_since_last_heard_from_discrete = 'red' THEN -1
    ELSE 0 END) * 0.25
    ) * 0.2

    +

    ((CASE 
        WHEN user_logged_in_last7 = 'green' THEN 1
        WHEN user_logged_in_last7 = 'yellow' THEN 0
        WHEN user_logged_in_last7 = 'red' THEN -1
    ELSE 0 END) * 0.25

    + 

    (CASE 
        WHEN at_least_50_actions_last30 = 'green' THEN 1
        WHEN at_least_50_actions_last30 = 'yellow' THEN 0
        WHEN at_least_50_actions_last30 = 'red' THEN -1
    ELSE 0 END) * 0.5

    +

    (CASE 
        WHEN at_least_50_actions_last7 = 'green' THEN 1
        WHEN at_least_50_actions_last7 = 'yellow' THEN 0
        WHEN at_least_50_actions_last7 = 'red' THEN -1
    ELSE 0 END) * 0.25
    ) * 0.35

    + 

    ((CASE 
        WHEN at_least_1_ai_template_used_last30 = 'green' THEN 1
        WHEN at_least_1_ai_template_used_last30 = 'yellow' THEN 0
        WHEN at_least_1_ai_template_used_last30 = 'red' THEN -1
    ELSE 0 END) * 0.7

    +

    (CASE 
        WHEN at_least_1_ai_block_trained_last30 = 'green' THEN 1
        WHEN at_least_1_ai_block_trained_last30 = 'yellow' THEN 0
        WHEN at_least_1_ai_block_trained_last30 = 'red' THEN -1
    ELSE 0 END) * 0.3
    ) * 0.30

    +

    ((CASE 
        WHEN days_since_onboarded_discrete = 'green' THEN 1
        WHEN days_since_onboarded_discrete = 'yellow' THEN 0
        WHEN days_since_onboarded_discrete = 'red' THEN -1
    ELSE 0 END) * 0.5

    +

    (CASE 
        WHEN milestone_1_ai_block_trained = 'green' THEN 1
        WHEN milestone_1_ai_block_trained = 'yellow' THEN 0
        WHEN milestone_1_ai_block_trained = 'red' THEN -1
    ELSE 0 END) * 0.5
    ) * 0.15 AS score_value

    FROM joined_tables


)

SELECT *
FROM joined_tables
JOIN score USING(email)


