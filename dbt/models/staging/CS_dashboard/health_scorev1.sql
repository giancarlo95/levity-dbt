{# WITH prediction_models_classifier AS (

    SELECT 
        user_id,
        aiblock_id,
        classifier_id
    FROM 
        {{ref('prediction_models_classifier')}}

), prediction_models_prediction AS (

    SELECT
        user_id,
        workspace_id,
        prediction_id,
        is_hitl,
        origin,
        workflow_id,
        date_prediction_made,
        classifier_id
    FROM 
        {{ref('prediction_models_prediction')}}

), datasets_dataset AS (

    SELECT
        user_id,
        workspace_id,
        aiblock_id,
        is_template,
        aiblock_name,
        aiblock_description
    FROM 
        {{ref('datasets_dataset')}}

), workspaces AS (

    SELECT
        workspace_id
    FROM
        {{ref('workspaces')}}
        
), final AS (

    SELECT
        IFNULL(pmp.user_id, pmc.user_id)                      AS user_id,
        pmp.workspace_id,
        pmc.aiblock_id                                        AS aiblock_id,
        is_template,
        aiblock_name,
        aiblock_description,
        is_hitl,
        origin,
        workflow_id,
        TIMESTAMP_TRUNC(pmp.date_prediction_made, HOUR)       AS relevant_day_hour,
        COUNT(pmp.prediction_id)                              AS total_predictions,
        MAX(date_prediction_made)                             AS time_stamp
    FROM prediction_models_prediction pmp
    INNER JOIN prediction_models_classifier pmc ON pmp.classifier_id = pmc.classifier_id
    INNER JOIN datasets_dataset dd ON dd.aiblock_id=pmc.aiblock_id
    WHERE TIMESTAMP_TRUNC(pmp.date_prediction_made, HOUR) = TIMESTAMP_TRUNC(TIMESTAMP_SUB(CURRENT_TIMESTAMP(),INTERVAL 2 HOUR), HOUR)
    GROUP BY 
        1, 
        2, 
        3, 
        4,
        5,
        6,
        7,
        8,
        9,
        10

)

SELECT
    user_id,
    final.workspace_id,
    aiblock_id,
    is_template,
    aiblock_name,
    aiblock_description,
    is_hitl,
    origin,
    workflow_id,
    total_predictions,
    time_stamp
FROM final
INNER JOIN workspaces oa ON final.workspace_id = oa.workspace_id #}


-- filter for specific lead status / lifecycle stage?

WITH engagement AS (

    SELECT 
        email,
        properties_company_value,
        properties_firstname_value,
        properties_lastname_value,
        CASE
            -- DATEDIFF() is what paradime expects, whereas BQ expects DATE_DIFF(later, earlier, DAY)
            WHEN DATE_DIFF(CURRENT_DATE(), DATE(properties_hs_last_sales_activity_timestamp_value), DAY) <= 30 THEN 'green'
            WHEN DATE_DIFF(CURRENT_DATE(), DATE(properties_hs_last_sales_activity_timestamp_value), DAY) BETWEEN 30 and 60 THEN 'yellow'
            WHEN DATE_DIFF(CURRENT_DATE(), DATE(properties_hs_last_sales_activity_timestamp_value), DAY) > 60 THEN 'red'  
        ELSE NULL END AS days_since_last_engagement,
        CASE 
            -- DATEDIFF() is what paradime expects, whereas BQ expects DATE_DIFF(later, earlier, DAY)
            WHEN DATE_DIFF(CURRENT_DATE(), DATE(properties_hs_email_last_reply_date_value), DAY) <= 30 THEN 'green'
            WHEN DATE_DIFF(CURRENT_DATE(), DATE(properties_hs_email_last_reply_date_value), DAY) BETWEEN 30 and 60 THEN 'yellow'
            WHEN DATE_DIFF(CURRENT_DATE(), DATE(properties_hs_email_last_reply_date_value), DAY) > 60 THEN 'red'  
        ELSE NULL END AS days_since_last_heard_from
        -- there's also the property 'last marketing email reply date' -> how to combine them?
    FROM 
        {{ref('hubspot_crm_contacts')}}
    
    -- where lead_status (or lifecycle_stage) is 'something'

),

login_last7 AS (

-- # user logged in last 7

    SELECT 
        user_id,
        CASE
            WHEN DATE_DIFF(CURRENT_DATE(), DATE(MAX(original_timestamp)), DAY) < 7 THEN 'green'
            ELSE 'red' 
        END AS user_logged_in_last7
    FROM {{ref('django_production_user_signed_in')}}
    GROUP BY user_id


),

num_people_in_account AS (

    SELECT
        user_id,
        COUNT(DISTINCT user_id)
    FROM {{ref('django_production_user_invited')}}
    GROUP BY user_id

),


actions_last30 AS (

    -- num actions

    SELECT
        user_id,
        CASE
            WHEN COUNT(id) >= 50 THEN 'green'
            ELSE 'red' 
        END AS at_least_50_actions_last30
    FROM 
        {{ref('django_production_actions')}}
    WHERE 
        -- DATEDIFF() is what paradime expects, whereas BQ expects DATE_DIFF(later, earlier, DAY)
        DATE_DIFF(CURRENT_DATE(), DATE(original_timestamp), DAY) BETWEEN 0 AND 30 
    GROUP BY user_id


),

actions_last7 AS (

    -- num actions

    SELECT
        user_id,
        CASE
            WHEN COUNT(id) >= 50 THEN 'green'
            ELSE 'red' 
        END AS at_least_50_actions_last7
    FROM 
        {{ref('django_production_actions')}}
    WHERE 
        -- DATEDIFF() is what paradime expects, whereas BQ expects DATE_DIFF(later, earlier, DAY)
        DATE_DIFF(CURRENT_DATE(), DATE(original_timestamp), DAY) BETWEEN 0 AND 7 
    GROUP BY user_id


),

ai_block_trained_last30 AS (

    SELECT
        user_id,
        {# COUNT(id) #}
        CASE 
            WHEN COUNT(id) >= 1 THEN 'green'
            ELSE 'red' 
        END AS count_ai_blocks_trained_last30  
    FROM 
        {{ref('django_production_ai_block_trained')}}
    WHERE
        -- DATEDIFF() is what paradime expects, whereas BQ expects DATE_DIFF(later, earlier, DAY)
        DATE_DIFF(CURRENT_DATE(), DATE(original_timestamp), DAY) BETWEEN 0 AND 30
    GROUP BY user_id 


), 

ai_template_used_last30 AS (

    SELECT
        user_id,
        {# COUNT(id) #}
        CASE 
            WHEN COUNT(id) >= 1 THEN 'green'
            ELSE 'red' 
        END AS count_ai_templates_used_last30
    FROM 
        {{ref('django_production_ai_block_template')}}
    WHERE
        -- DATEDIFF() is what paradime expects, whereas BQ expects DATE_DIFF(later, earlier, DAY)
        DATE_DIFF(CURRENT_DATE(), DATE(original_timestamp), DAY) BETWEEN 0 AND 30
    GROUP BY user_id

),

days_since_onboarding AS (

-- days since onboarding
    SELECT
        user_id,
        CASE
            WHEN DATE_DIFF(CURRENT_DATE(), DATE(original_timestamp), DAY) > 180 THEN 'green'
            WHEN DATE_DIFF(CURRENT_DATE(), DATE(original_timestamp), DAY) BETWEEN 90 AND 180 THEN 'yellow'
            {# WHEN DATE_DIFF(CURRENT_DATE(), DATE(original_timestamp), DAY) < 90 'red' #}
        ELSE 'red' END AS days_since_onboarded
    FROM 
        {{ref('django_production_user_onboarded')}}

),

-- days_in_onboarding AS (

    -- not 100% sure where to get this from based on notion page


--)

users AS (

    SELECT 
        *
    FROM
        {{ref('users')}}

)

SELECT
    e.email,
    e.days_since_last_engagement,
    e.days_since_last_heard_from,
    l.user_logged_in_last7,
    a30.at_least_50_actions_last30,
    a7.at_least_50_actions_last7,
    tr.count_ai_blocks_trained_last30,
    te.count_ai_templates_used_last30,
    o.days_since_onboarded
FROM engagement e
LEFT JOIN users u ON u.user_email_address=e.email
LEFT JOIN login_last7 l ON l.user_id=u.user_id
LEFT JOIN actions_last30 a30 ON a30.user_id=u.user_id
LEFT JOIN actions_last7 a7 ON a7.user_id=u.user_id
LEFT JOIN ai_block_trained_last30 tr ON tr.user_id=u.user_id
LEFT JOIN ai_template_used_last30 te ON te.user_id=u.user_id
LEFT JOIN days_since_onboarding o ON o.user_id=u.user_id

LEFT JOIN num_people_in_account n ON n.user_id=u.user_id

WHERE
    e.email NOT LIKE '%@levity.ai'