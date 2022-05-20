{{
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
        NOT(a.event IN ("label_created", "label_deleted", "user_signed_up", "email_confirmed", "typeform_filled", "user_onboarded"))
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
        "green" AS milestone_1_ai_block_trained
    FROM 
       {{ref('trained_ai_block_user')}}

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
        DATE_DIFF(CURRENT_DATE(), DATE(MIN(original_timestamp)), DAY) AS days_since_onboarded,
        CASE
            WHEN DATE_DIFF(CURRENT_DATE(), DATE(MIN(original_timestamp)), DAY) > 180 THEN 'green'
            WHEN DATE_DIFF(CURRENT_DATE(), DATE(MIN(original_timestamp)), DAY) BETWEEN 90 AND 180 THEN 'yellow'
            WHEN DATE_DIFF(CURRENT_DATE(), DATE(MIN(original_timestamp)), DAY) < 90 THEN 'red'
            ELSE NULL
        END AS days_since_onboarded_discrete
    FROM 
        {{ref('django_production_user_onboarded')}}
    GROUP BY 
        user_id

),

days_in_onboarding AS (

    SELECT
    
        user_id,
        DATE_DIFF(DATE(MIN(o.original_timestamp)), DATE(MIN(s.original_timestamp)), DAY) AS days_in_onboarded,
        CASE
            WHEN DATE_DIFF(DATE(MIN(o.original_timestamp)), DATE(MIN(s.original_timestamp)), DAY) > 60 THEN 'red'
            WHEN DATE_DIFF(DATE(MIN(o.original_timestamp)), DATE(MIN(s.original_timestamp)), DAY) BETWEEN 30 AND 60 THEN 'yellow'
            WHEN DATE_DIFF(DATE(MIN(o.original_timestamp)), DATE(MIN(s.original_timestamp)), DAY) < 30 THEN 'green'
            ELSE NULL
        END AS days_in_onboarding_discrete
    FROM 
        {{ref('django_production_user_onboarded')}} o
    JOIN {{ref('django_production_user_signed_up')}} s USING(user_id)
    GROUP BY 
        user_id

),

users AS (

    SELECT 
        LOWER(user_email_address) AS email,
        user_id
    FROM
        {{ref('users')}}

),

joined_tables AS (

    SELECT
    * EXCEPT(milestone_1_ai_block_trained),
    COALESCE(milestone_1_ai_block_trained, "red") AS milestone_1_ai_block_trained,
    CASE 
        WHEN days_since_last_heard_from <= 30 THEN 'green'
        WHEN days_since_last_heard_from BETWEEN 30 and 60 THEN 'yellow'
        WHEN days_since_last_heard_from > 60 THEN 'red'
        ELSE NULL
    END AS days_since_last_heard_from_discrete
    FROM engagement
    INNER JOIN users USING(email)
    LEFT JOIN login_last7 USING(user_id)
    LEFT JOIN actions_last30 USING(user_id)
    LEFT JOIN actions_last7 USING(user_id)
    LEFT JOIN ai_block_trained_last30 USING(user_id)
    LEFT JOIN milestone_ai_block_trained USING(user_id)
    LEFT JOIN ai_template_used_last30 USING(user_id)
    LEFT JOIN days_since_onboarding USING(user_id)
    LEFT JOIN days_in_onboarding USING(user_id)
    WHERE
        email NOT LIKE '%@levity.ai'

), engagement_score AS (

    SELECT email,

    (CASE 
        WHEN days_since_last_engagement_discrete = 'green' THEN 1
        WHEN days_since_last_engagement_discrete = 'yellow' THEN 0.5
        WHEN days_since_last_engagement_discrete = 'red' THEN 0
    ELSE 0 END) * 0.75

    + 

    (CASE 
        WHEN days_since_last_heard_from_discrete = 'green' THEN 1
        WHEN days_since_last_heard_from_discrete = 'yellow' THEN 0.5
        WHEN days_since_last_heard_from_discrete = 'red' THEN 0
    ELSE 0 END) * 0.25 AS engagement_score_value

    FROM joined_tables


), usage_score AS (

    SELECT email,

    (CASE 
        WHEN user_logged_in_last7 = 'green' THEN 1
        WHEN user_logged_in_last7 = 'red' THEN 0
    ELSE 0 END) * 0.25

    + 

    (CASE 
        WHEN at_least_50_actions_last30 = 'green' THEN 1
        WHEN at_least_50_actions_last30 = 'red' THEN 0
    ELSE 0 END) * 0.5

    +

    (CASE 
        WHEN at_least_50_actions_last7 = 'green' THEN 1
        WHEN at_least_50_actions_last7 = 'red' THEN 0
    ELSE 0 END) * 0.25 AS usage_score_value

    FROM joined_tables


), adoption_score AS (

    SELECT email,

    (CASE 
        WHEN at_least_1_ai_template_used_last30 = 'green' THEN 1
        WHEN at_least_1_ai_template_used_last30 = 'red' THEN 0
    ELSE 0 END) * 0.7

    +

    (CASE 
        WHEN at_least_1_ai_block_trained_last30 = 'green' THEN 1
        WHEN at_least_1_ai_block_trained_last30 = 'red' THEN 0
    ELSE 0 END) * 0.3 AS adoption_score_value

    FROM joined_tables


), journey_score AS (

    SELECT email,

    (CASE 
        WHEN days_since_onboarded_discrete = 'green' THEN 1
        WHEN days_since_onboarded_discrete = 'yellow' THEN 0.5
        WHEN days_since_onboarded_discrete = 'red' THEN 0
    ELSE 0 END) * 0.25

    +

    (CASE 
        WHEN days_in_onboarding_discrete = 'green' THEN 1
        WHEN days_in_onboarding_discrete = 'yellow' THEN 0.5
        WHEN days_in_onboarding_discrete = 'red' THEN 0
    ELSE 0 END) * 0.25

    +

    (CASE 
        WHEN milestone_1_ai_block_trained = 'green' THEN 1
        WHEN milestone_1_ai_block_trained = 'red' THEN 0
    ELSE 0 END) * 0.5 AS journey_score_value

    FROM joined_tables


)

SELECT 
    *
FROM 
    joined_tables
JOIN engagement_score USING(email)
JOIN usage_score USING(email)
JOIN adoption_score USING(email)
JOIN journey_score USING(email)


