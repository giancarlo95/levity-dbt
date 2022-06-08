{{
  config(
    materialized = 'table',
    )
}}

WITH engagement_first AS (

    SELECT 
        LOWER(email) AS email,
        DATE_DIFF(CURRENT_DATE(), DATE(properties_hs_last_sales_activity_timestamp_value), DAY) AS engagement_days_since_last_engagement,
        CASE
            WHEN DATE_DIFF(CURRENT_DATE(), DATE(properties_hs_last_sales_activity_timestamp_value), DAY) <= 30 THEN 1
            WHEN DATE_DIFF(CURRENT_DATE(), DATE(properties_hs_last_sales_activity_timestamp_value), DAY) BETWEEN 30 and 60 THEN 0.5 
            WHEN DATE_DIFF(CURRENT_DATE(), DATE(properties_hs_last_sales_activity_timestamp_value), DAY) > 60 THEN 0
            ELSE NULL 
        END AS engagement_days_since_last_engagement_score,
        DATE_DIFF(CURRENT_DATE(), DATE(GREATEST(COALESCE(properties_hs_sales_email_last_replied_value, properties_hs_email_last_reply_date_value), COALESCE(properties_hs_email_last_reply_date_value, properties_hs_sales_email_last_replied_value))), DAY) AS engagement_days_since_last_heard_from
    FROM 
        {{ref('hubspot_crm_contacts')}}
    WHERE   
        properties_is_onboarded_value = "yes"

), engagement_second AS (

    SELECT 
        *,
        CASE 
            WHEN engagement_days_since_last_heard_from <= 30 THEN 1
            WHEN engagement_days_since_last_heard_from BETWEEN 30 and 60 THEN 0.5
            WHEN engagement_days_since_last_heard_from > 60 THEN 0
            ELSE NULL
        END AS engagement_days_since_last_heard_from_score
    FROM
        engagement_first

),

login_last7 AS (

    SELECT 
        s.user_id,
        DATE_DIFF(CURRENT_DATE(), DATE(MAX(s.original_timestamp)), DAY) AS usage_days_since_last_login,
        CASE
            WHEN DATE_DIFF(CURRENT_DATE(), DATE(MAX(s.original_timestamp)), DAY) < 7 THEN 1
            ELSE 0 
        END AS usage_days_since_last_login_score
    FROM 
        {{ref('django_production_actions')}} s
    LEFT JOIN 
        {{ref('django_production_datapoints_added')}} d USING(id)
    WHERE 
        NOT(s.event IN ("predictions_done", "user_signed_up", "email_confirmed", "typeform_filled", "user_onboarded"))
        AND NOT(s.event = "datapoints_added" AND d.is_human_in_the_loop = "yes")
    GROUP BY 
        s.user_id

),

actions_last30 AS (

    SELECT
        a.user_id,
        COUNT(a.id) AS usage_actions_last30,
        CASE
            WHEN COUNT(a.id) >= 50 THEN 1
            ELSE 0 
        END AS usage_actions_last30_score
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
        COUNT(a.id) AS usage_actions_last7,
        CASE
            WHEN COUNT(a.id) >= 50 THEN 1
            ELSE 0 
        END AS usage_actions_last7_score
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
        COUNT(id) AS adoption_ai_block_trained_last30,
        CASE 
            WHEN COUNT(id) >= 1 THEN 1
            ELSE 0 
        END AS adoption_ai_block_trained_last30_score  
    FROM 
        {{ref('django_production_ai_block_trained')}}
    WHERE
        DATE_DIFF(CURRENT_DATE(), DATE(original_timestamp), DAY) BETWEEN 0 AND 30
    GROUP BY 
        user_id 

),

ai_template_used_last30 AS (

    SELECT
        pmc.user_id,
        COUNT(pd.id) AS adoption_ai_template_used_last30,
        CASE 
            WHEN COUNT(pd.id) >= 1 THEN 1
            ELSE 0 
        END AS adoption_ai_template_used_last30_score
    FROM 
        {{ref('django_production_predictions_done')}} pd
    JOIN {{ref('prediction_models_classifier')}} pmc ON pd.dataset_id=pmc.aiblock_id
    WHERE
        DATE_DIFF(CURRENT_DATE(), DATE(original_timestamp), DAY) BETWEEN 0 AND 30
        AND NOT(origin = "test_tab")
        AND is_template = "yes"
    GROUP BY 
        pmc.user_id

), 

milestone_ai_block_trained AS (

    SELECT
        user_id,
        1 AS journey_milestone_ai_block_trained_score
    FROM 
       {{ref('trained_ai_block_user')}}

),

milestone_ai_block_connected AS (

    SELECT  
        pmc.user_id,
        1 AS journey_milestone_ai_block_connected_score
    FROM 
        {{ref('django_production_predictions_done')}} pd
    JOIN {{ref('prediction_models_classifier')}} pmc ON pd.dataset_id=pmc.aiblock_id
    WHERE
        NOT(origin = "test_tab")
    GROUP BY 
        pmc.user_id


),

days_since_onboarding AS (

    SELECT
        user_id,
        DATE_DIFF(CURRENT_DATE(), DATE(MIN(original_timestamp)), DAY) AS journey_days_since_onboarding,
        CASE
            WHEN DATE_DIFF(CURRENT_DATE(), DATE(MIN(original_timestamp)), DAY) > 180 THEN 1
            WHEN DATE_DIFF(CURRENT_DATE(), DATE(MIN(original_timestamp)), DAY) BETWEEN 90 AND 180 THEN 0.5
            WHEN DATE_DIFF(CURRENT_DATE(), DATE(MIN(original_timestamp)), DAY) < 90 THEN 0
            ELSE NULL
        END AS journey_days_since_onboarding_score
    FROM 
        {{ref('django_production_user_onboarded')}}
    GROUP BY 
        user_id

),

days_in_onboarding AS (

    SELECT
    
        user_id,
        DATE_DIFF(DATE(MIN(o.original_timestamp)), DATE(MIN(s.original_timestamp)), DAY) AS journey_days_in_onboarding,
        CASE
            WHEN DATE_DIFF(DATE(MIN(o.original_timestamp)), DATE(MIN(s.original_timestamp)), DAY) > 60 THEN 0
            WHEN DATE_DIFF(DATE(MIN(o.original_timestamp)), DATE(MIN(s.original_timestamp)), DAY) BETWEEN 30 AND 60 THEN 0.5
            WHEN DATE_DIFF(DATE(MIN(o.original_timestamp)), DATE(MIN(s.original_timestamp)), DAY) < 30 THEN 1
            ELSE NULL
        END AS journey_days_in_onboarding_score
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
    *
    FROM engagement_second
    INNER JOIN users USING(email)
    LEFT JOIN login_last7 USING(user_id)
    LEFT JOIN actions_last30 USING(user_id)
    LEFT JOIN actions_last7 USING(user_id)
    LEFT JOIN ai_block_trained_last30 USING(user_id)
    LEFT JOIN milestone_ai_block_trained USING(user_id)
    LEFT JOIN milestone_ai_block_connected USING(user_id)
    LEFT JOIN ai_template_used_last30 USING(user_id)
    LEFT JOIN days_since_onboarding USING(user_id)
    LEFT JOIN days_in_onboarding USING(user_id)
    WHERE
        email NOT LIKE '%@levity.ai'

), add_four_scores AS (

    SELECT 
    *,
    COALESCE(engagement_days_since_last_engagement_score, 0) * 0.75 + COALESCE(engagement_days_since_last_heard_from_score, 0) * 0.25 AS engagement_score,
    COALESCE(usage_days_since_last_login_score, 0) * 0.25 + COALESCE(usage_actions_last30_score, 0) * 0.5 + COALESCE(usage_actions_last7_score, 0) * 0.25 AS usage_score,
    COALESCE(adoption_ai_template_used_last30_score, 0) * 0.7 + COALESCE(adoption_ai_block_trained_last30_score, 0) * 0.3 AS adoption_score,
    COALESCE(journey_days_since_onboarding_score, 0) * 0.25 + COALESCE(journey_days_in_onboarding_score, 0) * 0.25 + COALESCE(journey_milestone_ai_block_trained_score, 0) * 0.25 + COALESCE(journey_milestone_ai_block_connected_score, 0) * 0.25 AS journey_score
    FROM joined_tables


), add_final_score AS (

    SELECT 
        *,
        engagement_score + usage_score + adoption_score + journey_score AS overall_score,
        PERCENTILE_CONT(engagement_score + usage_score + adoption_score + journey_score, 0.9) OVER() AS overall_score_90th_perc
    FROM 
        add_four_scores

) 

SELECT 
    *,
    CASE    
        WHEN overall_score >= overall_score_90th_perc THEN "green"
        WHEN overall_score < overall_score_90th_perc AND overall_score > 0 THEN "yellow"
        ELSE "red"
    END AS overall_score_range
FROM 
    add_final_score
ORDER BY
    overall_score DESC
